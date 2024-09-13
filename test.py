from flask import Flask, render_template, request, redirect, url_for
from flask_socketio import SocketIO, emit
import os

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    # Handle file upload
    if 'file' not in request.files:
        return redirect(url_for('index'))
    file = request.files['file']
    if file.filename == '':
        return redirect(url_for('index'))
    if file:
        file_path = os.path.join('./upload', file.filename)
        file.save(file_path)
        socketio.emit('log', {'data': f'File {file.filename} uploaded successfully.'})
        # Add any other processing here...
        return redirect(url_for('index'))

@socketio.on('connect')
def handle_connect():
    print("Client connected")

if __name__ == '__main__':
    # 允许使用 Werkzeug 服务器进行开发测试
    socketio.run(app, debug=True, allow_unsafe_werkzeug=True,port=5001)
