import os
import json
import uuid
import re
import threading
from datetime import datetime
from flask import Flask, render_template, request, jsonify, send_from_directory
from flask_socketio import SocketIO, emit, join_room, leave_room
from werkzeug.utils import secure_filename
import html
from collections import defaultdict
from time import time


app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', os.urandom(24).hex())
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_FOLDER = os.path.join(BASE_DIR, 'uploads')
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

db_locks = defaultdict(threading.Lock)


def get_lock(path):
    """Get a lock specific to a file path."""
    return db_locks[path]


USERS_DB = {
    "username":   {"password": "password",       "is_admin": True}
}

ROOMS_DB = {
    "room1": {"id": "room1", "name": "ASMA", "max_members": 4, "avatar_color": "#0AD0DF",
              "members_count": 0, "is_dm": False, "owner": "ARTIN", "invite_link": "invite_asma"},
}


active_connections = {}
auth_tokens = {}

MESSAGES_FILE = os.path.join(BASE_DIR, 'messages.json')
DM_FILE = os.path.join(BASE_DIR, 'DM.json')
DM_MESSAGES_DIR = os.path.join(BASE_DIR, 'DM messages')
os.makedirs(DM_MESSAGES_DIR, exist_ok=True)

message_timestamps = defaultdict(list)
RATE_LIMIT_MESSAGES = 10
RATE_LIMIT_WINDOW = 10
RATE_LIMIT_UPLOADS = 5
RATE_LIMIT_UPLOAD_WINDOW = 60

ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif', 'webp'}



def get_ip_address():
    if request.headers.getlist("X-Forwarded-For"):
        return request.headers.getlist("X-Forwarded-For").split(',').strip()
    return request.remote_addr or "unknown"


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1).lower() in ALLOWED_EXTENSIONS


def safe_get_profile(sid):
    """FIX 3: Safe accessor for active_connections."""
    return active_connections.get(sid) or {}


def check_rate_limit(ip, limit=RATE_LIMIT_MESSAGES, window=RATE_LIMIT_WINDOW):
    now = time()
    timestamps = message_timestamps[ip]
    timestamps[:] = [t for t in timestamps if now - t < window]
    if len(timestamps) >= limit:
        return False
    timestamps.append(now)
    return True


def cleanup_stale_rate_limit_entries():
    """FIX 5: Periodic cleanup of empty IP entries."""
    cutoff = time() - 3600
    empty_keys = [ip for ip, ts in message_timestamps.items()
                  if not ts or max(ts) < cutoff]
    for k in empty_keys:
        message_timestamps.pop(k, None)


def load_json(path, default):
    if not os.path.exists(path):
        return default
    try:
        with get_lock(path):
            with open(path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                return json.loads(content) if content else default
    except (json.JSONDecodeError, IOError) as e:
        print(f"[load_json] {path}: {e}")
        return default


def save_json(path, data):
    try:
        with get_lock(path):
            tmp_path = path + '.tmp'
            with open(tmp_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
            os.replace(tmp_path, path)
    except IOError as e:
        print(f"[save_json] {path}: {e}")


def load_messages():
    return load_json(MESSAGES_FILE, [])


def save_messages(messages):
    save_json(MESSAGES_FILE, messages)


def load_dm_registry():
    return load_json(DM_FILE, {})


def save_dm_registry(registry):
    save_json(DM_FILE, registry)


def load_dm_messages(chat_id):
    chat_file = os.path.join(DM_MESSAGES_DIR, f"{chat_id}.json")
    return load_json(chat_file, [])


def save_dm_messages(chat_id, messages_list):
    chat_file = os.path.join(DM_MESSAGES_DIR, f"{chat_id}.json")
    save_json(chat_file, messages_list)


def sanitize_message(text):
    return html.escape(text, quote=False)


def format_mentions(text):
    """Add <strong> tags around @MENTIONS. Input is already HTML-escaped."""
    return re.sub(r'@([A-Z0-9]+)',
                  lambda m: f'<strong class="mention">@{m.group(1)}</strong>',
                  text)


def get_filtered_rooms_for_client(username):
    """Return all public rooms plus DMs the user participates in."""
    visible = list(ROOMS_DB.values())

    dm_db = load_dm_registry()
    for dm_id, info in dm_db.items():
        parties = dm_id.split("_to_")
        if len(parties) == 2 and username in parties:
            partner = parties if parties == username else parties
            visible.append({
                "id": dm_id,
                "name": f"💬 {partner}",
                "max_members": 2,
                "avatar_color": info.get("avatar_color", "#2f6da3"),
                "members_count": 0,
                "is_dm": True,
                "owner": None,
                "invite_link": dm_id,
            })
    return visible


def update_room_occupancy_metrics(room_id):
    """Recount members in a room and notify clients."""
    current_occupants = [sid for sid, data in active_connections.items()
                         if data.get('room_id') == room_id]

    if room_id in ROOMS_DB:
        ROOMS_DB[room_id]['members_count'] = len(current_occupants)
        capacity = ROOMS_DB[room_id]['max_members']
    else:
        capacity = 2

    socketio.emit('update_room_members',
                  {'count': len(current_occupants), 'capacity': capacity},
                  room=room_id)
    broadcast_lists_globally()


def broadcast_lists_globally():
    """Push updated room list & online users to everyone."""
    online_users = list({d['username'] for d in active_connections.values()
                         if d.get('username')})
    for sid, data in list(active_connections.items()):
        if data.get('username'):
            try:
                socketio.emit('available_rooms',
                              {'rooms': get_filtered_rooms_for_client(data['username'])},
                              room=sid)
                socketio.emit('global_users_list', {'users': online_users}, room=sid)
            except Exception as e:
                print(f"[broadcast_lists_globally] {sid}: {e}")


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/uploads/<path:filename>')
def uploaded_file(filename):
    return send_from_directory(app.config['UPLOAD_FOLDER'], secure_filename(filename))


@app.route('/upload-image', methods=['POST'])
def upload_image():
    if 'image' not in request.files:
        return jsonify({'error': 'No image file'}), 400

    file = request.files['image']
    username = request.form.get('username', '').strip().upper()
    auth_token = request.form.get('auth_token', '').strip()
    room_id = request.form.get('room_id', '').strip()

    if not username or not room_id or not auth_token:
        return jsonify({'error': 'Missing verification context'}), 400

    target_sid = auth_tokens.get(auth_token)
    if not target_sid or target_sid not in active_connections:
        return jsonify({'error': 'Invalid or expired token'}), 403
    if safe_get_profile(target_sid).get('username') != username:
        return jsonify({'error': 'Username/token mismatch'}), 403

    ip = get_ip_address()
    if not check_rate_limit(f"upload:{ip}",
                            limit=RATE_LIMIT_UPLOADS,
                            window=RATE_LIMIT_UPLOAD_WINDOW):
        return jsonify({'error': 'Too many uploads, slow down.'}), 429

    if "_to_" in room_id:
        allowed_users = room_id.split("_to_")
        if username not in allowed_users:
            return jsonify({'error': 'Unauthorized for this DM'}), 403

    if safe_get_profile(target_sid).get('room_id') != room_id:
        return jsonify({'error': 'You must be in the room to upload.'}), 403

    if not (file and allowed_file(file.filename)):
        return jsonify({'error': 'Invalid file type'}), 400

    ext = file.filename.rsplit('.', 1).lower()
    if ext not in ALLOWED_EXTENSIONS:
        return jsonify({'error': 'Invalid extension'}), 400
    filename = f"{uuid.uuid4().hex}.{ext}"
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    file.save(filepath)

    msg_data = {
        'id': str(uuid.uuid4()),
        'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'message': '',
        'image_url': f'/uploads/{filename}',
        'image_name': secure_filename(file.filename),
        'username': username,
        'type': 'image',
        'room': room_id,
        'is_edited': False,
    }

    if "_to_" in room_id:
        dm_history = load_dm_messages(room_id)
        dm_history.append(msg_data)
        save_dm_messages(room_id, dm_history)
    else:
        history = load_messages()
        history.append(msg_data)
        save_messages(history[-1000:])

    socketio.emit('new_message', msg_data, room=room_id)
    return jsonify({'success': True})


@socketio.on('connect')
def handle_connect():
    active_connections[request.sid] = {
        'username': None, 'room_id': None,
        'is_admin': False, 'ip': get_ip_address(),
        'auth_token': None,
    }


@socketio.on('login_attempt')
def handle_login(data):
    sid = request.sid
    username = str(data.get('username', '')).strip().upper()
    password = str(data.get('password', ''))

    if username not in USERS_DB or USERS_DB[username]['password'] != password:
        emit('login_error', {'message': 'Invalid credentials.'})
        return

    token = uuid.uuid4().hex
    profile = active_connections.get(sid)
    if not profile:
        return
    profile['username'] = username
    profile['is_admin'] = USERS_DB[username]['is_admin']
    profile['auth_token'] = token
    auth_tokens[token] = sid

    emit('login_success', {
        'username': username,
        'is_admin': profile['is_admin'],
        'auth_token': token,
    })
    broadcast_lists_globally()


@socketio.on('get_available_rooms')
def handle_get_rooms():
    sid = request.sid
    profile = safe_get_profile(sid)
    if profile.get('username'):
        socketio.emit('available_rooms',
                      {'rooms': get_filtered_rooms_for_client(profile['username'])},
                      room=sid)


@socketio.on('create_new_room')
def handle_create_room(data):
    sid = request.sid
    profile = safe_get_profile(sid)
    username = profile.get('username')
    if not username:
        return

    name = str(data.get('room_name', '')).strip()
    if not name:
        emit('settings_error', {'message': 'Room name cannot be empty.'})
        return
    if len(name) > 50:
        emit('settings_error', {'message': 'Room name too long (max 50 chars).'})
        return

    try:
        max_members = int(data.get('max_members', 10))
    except (TypeError, ValueError):
        emit('settings_error', {'message': 'Invalid max_members value.'})
        return
    if max_members < 2 or max_members > 100:
        emit('settings_error', {'message': 'max_members must be between 2 and 100.'})
        return

    color = str(data.get('avatar_color', '#5288c1'))
    if not re.match(r'^#[0-9A-Fa-f]{6}$', color):
        color = '#5288c1'

    room_id = f"room_{uuid.uuid4().hex[:8]}"
    invite = f"invite_{uuid.uuid4().hex[:12]}"

    ROOMS_DB[room_id] = {
        "id": room_id, "name": name, "max_members": max_members,
        "avatar_color": color, "members_count": 0, "is_dm": False,
        "owner": username, "invite_link": invite,
    }
    broadcast_lists_globally()


@socketio.on('start_direct_message')
def handle_dm(data):
    sid = request.sid
    profile = safe_get_profile(sid)
    sender = profile.get('username')
    target_user = str(data.get('target_user', '')).strip().upper()

    if not sender or not target_user or sender == target_user:
        emit('settings_error', {'message': 'Invalid DM target.'})
        return
    if target_user not in USERS_DB:
        emit('settings_error', {'message': f'User "{target_user}" does not exist.'})
        return

    dm_db = load_dm_registry()

    id_1 = f"{sender}_to_{target_user}"
    id_2 = f"{target_user}_to_{sender}"
    chosen = id_1
    if id_2 in dm_db:
        chosen = id_2
    elif id_1 in dm_db:
        chosen = id_1

    if chosen not in dm_db:
        dm_db[chosen] = {
            "id": chosen,
            "sender_origin": sender,
            "target_destination": target_user,
            "avatar_color": "#2f6da3",
        }
        save_dm_registry(dm_db)
        save_dm_messages(chosen, [])

    socketio.emit('dm_room_ready',
                  {'room_id': chosen, 'name': f"💬 {target_user}"},
                  room=sid)
    broadcast_lists_globally()


@socketio.on('invite_user_by_link')
def handle_link_join(data):
    sid = request.sid
    profile = safe_get_profile(sid)
    username = profile.get('username')
    if not username:
        return

    invite_key = str(data.get('room_id', '')).strip()

    if "_to_" in invite_key:
        parties = invite_key.split("_to_")
        if username in parties:
            partner = parties if parties == username else parties
            socketio.emit('invite_link_validated',
                          {'room_id': invite_key, 'name': f"💬 {partner}"},
                          room=sid)
        else:
            socketio.emit('settings_error',
                          {'message': 'Access Denied: not your DM.'}, room=sid)
        return

    target_room = None
    for r_id, r in ROOMS_DB.items():
        if r_id == invite_key or r.get('invite_link') == invite_key:
            target_room = r
            break

    if not target_room:
        socketio.emit('settings_error', {'message': 'Invalid invite link.'}, room=sid)
        return

    occupants = [s for s, d in active_connections.items()
                 if d.get('room_id') == target_room['id']]
    if len(occupants) >= target_room['max_members']:
        socketio.emit('settings_error',
                      {'message': f"Room is full ({target_room['max_members']} max)."},
                      room=sid)
        return

    socketio.emit('invite_link_validated',
                  {'room_id': target_room['id'], 'name': target_room['name']},
                  room=sid)


@socketio.on('update_room_settings')
def handle_update_settings(data):
    sid = request.sid
    profile = safe_get_profile(sid)
    username = profile.get('username')
    is_global_admin = profile.get('is_admin', False)
    room_id = data.get('room_id')

    if room_id not in ROOMS_DB or not username:
        return
    room = ROOMS_DB[room_id]
    if room.get('owner') != username and not is_global_admin:
        socketio.emit('settings_error', {'message': 'Unauthorized.'}, room=sid)
        return

    if data.get('name') and str(data['name']).strip():
        new_name = str(data['name']).strip()[:50]
        room['name'] = new_name

    if data.get('avatar_color') and re.match(r'^#[0-9A-Fa-f]{6}$', str(data['avatar_color'])):
        room['avatar_color'] = str(data['avatar_color'])

    if data.get('max_members'):
        try:
            mm = int(data['max_members'])
            if 2 <= mm <= 100:
                room['max_members'] = mm
        except (TypeError, ValueError):
            pass

    socketio.emit('room_details_updated', {
        'room_id': room_id,
        'name': room['name'],
        'avatar_color': room['avatar_color'],
    }, room=room_id)

    broadcast_lists_globally()


@socketio.on('generate_new_invite_link')
def handle_new_link(data):
    sid = request.sid
    profile = safe_get_profile(sid)
    username = profile.get('username')
    is_global_admin = profile.get('is_admin', False)
    room_id = data.get('room_id')

    if room_id not in ROOMS_DB or not username:
        return
    room = ROOMS_DB[room_id]
    if room.get('owner') != username and not is_global_admin:
        return

    new_key = f"invite_{uuid.uuid4().hex[:12]}"
    room['invite_link'] = new_key
    socketio.emit('new_invite_link_generated', {'invite_link': new_key}, room=sid)
    broadcast_lists_globally()


@socketio.on('kick_user_from_room')
def handle_kick_user(data):
    sid = request.sid
    profile = safe_get_profile(sid)
    username = profile.get('username')
    is_global_admin = profile.get('is_admin', False)
    room_id = data.get('room_id')
    target_username = str(data.get('target_user', '')).strip().upper()

    if room_id not in ROOMS_DB or not username:
        return
    room = ROOMS_DB[room_id]
    if room.get('owner') != username and not is_global_admin:
        socketio.emit('settings_error', {'message': 'Unauthorized.'}, room=sid)
        return

    if room.get('owner') == target_username:
        socketio.emit('settings_error',
                      {'message': 'Cannot kick the room owner.'}, room=sid)
        return

    target_sid = None
    for csid, cp in active_connections.items():
        if cp.get('username') == target_username and cp.get('room_id') == room_id:
            target_sid = csid
            break

    if target_sid:
        leave_room(room_id, sid=target_sid)
        if active_connections.get(target_sid):
            active_connections[target_sid]['room_id'] = None
        socketio.emit('you_were_kicked', {'room_id': room_id}, room=target_sid)
        emit('user_left_room', {'username': target_username},
             room=room_id, include_self=True)
        update_room_occupancy_metrics(room_id)


@socketio.on('join_chat_room')
def handle_join_room(data):
    sid = request.sid
    profile = safe_get_profile(sid)
    username = profile.get('username')
    room_id = data.get('room_id')

    if not username or not room_id:
        return

    if "_to_" in room_id:
        parties = room_id.split("_to_")
        if username not in parties:
            socketio.emit('settings_error',
                          {'message': 'Unauthorized DM access.'}, room=sid)
            return
        profile['room_id'] = room_id
        join_room(room_id, sid=sid)
        history = load_dm_messages(room_id)
        socketio.emit('chat_history', {'room_id': room_id, 'messages': history},
                      room=sid)
        update_room_occupancy_metrics(room_id)
        return

    if room_id not in ROOMS_DB:
        return
    room = ROOMS_DB[room_id]

    occupants = [s for s, d in active_connections.items() if d.get('room_id') == room_id]
    if sid not in occupants and len(occupants) >= room['max_members']:
        socketio.emit('settings_error',
                      {'message': 'Room is at full capacity.'}, room=sid)
        return

    profile['room_id'] = room_id
    join_room(room_id, sid=sid)

    all_msgs = load_messages()
    room_history = [m for m in all_msgs if m.get('room') == room_id][-100:]
    socketio.emit('chat_history',
                  {'room_id': room_id, 'messages': room_history}, room=sid)

    emit('user_joined_room', {'username': username},
         room=room_id, include_self=False)
    update_room_occupancy_metrics(room_id)


@socketio.on('leave_chat_room')
def handle_leave_room(data):
    sid = request.sid
    profile = safe_get_profile(sid)
    if not profile:
        return

    room_id = (data or {}).get('room_id') or profile.get('room_id')
    if not room_id:
        return

    leave_room(room_id, sid=sid)
    if profile.get('room_id') == room_id:
        profile['room_id'] = None

    if "_to_" not in room_id and profile.get('username'):
        emit('user_left_room', {'username': profile['username']},
             room=room_id, include_self=False)

    update_room_occupancy_metrics(room_id)


@socketio.on('send_message')
def handle_chat(data):
    sid = request.sid
    profile = safe_get_profile(sid)
    room_id = (data or {}).get('room_id') or profile.get('room_id')

    if not profile.get('username') or not room_id:
        return
    if not check_rate_limit(profile.get('ip', 'unknown')):
        emit('settings_error', {'message': 'Slow down — rate limit exceeded.'}, room=sid)
        return

    message_text = str(data.get('message', '')).strip()
    if not message_text:
        return
    if len(message_text) > 2000:
        emit('settings_error', {'message': 'Message too long.'}, room=sid)
        return

    if "_to_" in room_id:
        parties = room_id.split("_to_")
        if profile['username'] not in parties:
            return

    msg_id = str(uuid.uuid4())
    clean_message = format_mentions(sanitize_message(message_text))
    mentioned_users = re.findall(r'@([A-Z0-9]+)', message_text)

    reply_to = data.get('reply_to')
    if reply_to is not None and not isinstance(reply_to, str):
        reply_to = None

    msg_data = {
        'id': msg_id,
        'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'message': clean_message,
        'username': profile['username'],
        'type': 'user',
        'reply_to': reply_to,
        'room': room_id,
        'mentioned': list(set(mentioned_users)),
        'is_edited': False,
    }

    if "_to_" in room_id:
        dm_history = load_dm_messages(room_id)
        dm_history.append(msg_data)
        save_dm_messages(room_id, dm_history)
    else:
        history = load_messages()
        history.append(msg_data)
        save_messages(history[-1000:])

    socketio.emit('new_message', msg_data, room=room_id)

    if mentioned_users:
        online_names = {d['username'] for d in active_connections.values()
                        if d.get('username')}
        for mentioned in set(mentioned_users):
            if mentioned in online_names:
                target_sid = next((s for s, d in active_connections.items()
                                   if d.get('username') == mentioned), None)
                if target_sid:
                    socketio.emit('you_were_mentioned', {
                        'msg_id': msg_id,
                        'room_id': room_id,
                        'by_user': profile['username'],
                    }, room=target_sid)


@socketio.on('delete_message')
def handle_delete(data):
    sid = request.sid
    profile = safe_get_profile(sid)
    msg_id = (data or {}).get('msg_id')
    room_id = profile.get('room_id')
    username = profile.get('username')
    is_admin = profile.get('is_admin', False)

    if not room_id or not msg_id:
        return

    is_dm = "_to_" in room_id
    if is_dm:
        messages = load_dm_messages(room_id)
        target = next((m for m in messages if m.get('id') == msg_id), None)
        if target and (target.get('username') == username or is_admin):
            updated = [m for m in messages if m.get('id') != msg_id]
            save_dm_messages(room_id, updated)
            socketio.emit('message_deleted', {'msg_id': msg_id}, room=room_id)
    else:
        history = load_messages()
        target = next((m for m in history if m.get('id') == msg_id), None)
        if target and (target.get('username') == username or is_admin):
            updated = [m for m in history if m.get('id') != msg_id]
            save_messages(updated)
            socketio.emit('message_deleted', {'msg_id': msg_id}, room=room_id)


@socketio.on('logout')
def handle_logout():
    sid = request.sid
    profile = safe_connections_get(sid)
    if profile and profile.get('auth_token'):
        auth_tokens.pop(profile['auth_token'], None)
    handle_disconnect()


@socketio.on('disconnect')
def handle_disconnect():
    sid = request.sid
    user_data = active_connections.pop(sid, None)
    if user_data:
        token = user_data.get('auth_token')
        if token:
            auth_tokens.pop(token, None)
        if user_data.get('room_id'):
            old_room = user_data['room_id']
            user_data['room_id'] = None
            update_room_occupancy_metrics(old_room)
    broadcast_lists_globally()


def safe_connections_get(sid):
    return active_connections.get(sid)

if __name__ == '__main__':
    debug_mode = os.environ.get('FLASK_DEBUG', '0') == '1'
    socketio.run(app, host='0.0.0.0', port=2585, debug=debug_mode, allow_unsafe_werkzeug=True)