import os
import json
import uuid
from datetime import datetime
from flask import Flask, request, jsonify, render_template_string

app = Flask(__name__)

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(DATA_DIR, exist_ok=True)

def get_state_path(item_id: str) -> str:
    # Ensure no path traversal and translate slashes to underscores
    clean_id = item_id.replace("/", "_")
    safe_id = "".join(c for c in clean_id if c.isalnum() or c in "-_")
    return os.path.join(DATA_DIR, f"state_{safe_id}.json")

@app.route("/", methods=["GET"])
def index():
    # Render a premium status page
    states_info = []
    if os.path.exists(DATA_DIR):
        for filename in os.listdir(DATA_DIR):
            if filename.startswith("state_") and filename.endswith(".json"):
                state_id = filename[6:-5]
                filepath = os.path.join(DATA_DIR, filename)
                try:
                    mtime = os.path.getmtime(filepath)
                    last_updated = datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %I:%M:%S %p")
                    size = os.path.getsize(filepath)
                    states_info.append({
                        "id": state_id,
                        "last_updated": last_updated,
                        "size": f"{size} bytes"
                    })
                except Exception:
                    pass
    
    # Premium Dark Mode Glassmorphism HTML
    html_content = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>BiPass State Server Dashboard</title>
        <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=swap" rel="stylesheet">
        <style>
            :root {
                --bg-color: #0d0e12;
                --card-bg: rgba(255, 255, 255, 0.03);
                --card-border: rgba(255, 255, 255, 0.08);
                --text-color: #eaecef;
                --text-muted: #848e9c;
                --primary: #f0b90b;
                --success: #0ecb81;
            }
            * {
                box-sizing: border-box;
                margin: 0;
                padding: 0;
            }
            body {
                font-family: 'Inter', sans-serif;
                background-color: var(--bg-color);
                color: var(--text-color);
                line-height: 1.6;
                padding: 40px 20px;
                display: flex;
                flex-direction: column;
                align-items: center;
                min-height: 100vh;
            }
            .container {
                max-width: 800px;
                width: 100%;
            }
            header {
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 40px;
                background: var(--card-bg);
                border: 1px solid var(--card-border);
                padding: 24px;
                border-radius: 16px;
                backdrop-filter: blur(10px);
            }
            h1 {
                font-size: 24px;
                font-weight: 700;
                background: linear-gradient(135deg, #fff 0%, #f0b90b 100%);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
            }
            .status-badge {
                display: inline-flex;
                align-items: center;
                background: rgba(14, 203, 129, 0.1);
                color: var(--success);
                padding: 6px 12px;
                border-radius: 20px;
                font-size: 14px;
                font-weight: 600;
                border: 1px solid rgba(14, 203, 129, 0.2);
            }
            .status-dot {
                width: 8px;
                height: 8px;
                background-color: var(--success);
                border-radius: 50%;
                margin-right: 8px;
                animation: pulse 2s infinite;
            }
            @keyframes pulse {
                0% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(14, 203, 129, 0.7); }
                70% { transform: scale(1); box-shadow: 0 0 0 8px rgba(14, 203, 129, 0); }
                100% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(14, 203, 129, 0); }
            }
            .card {
                background: var(--card-bg);
                border: 1px solid var(--card-border);
                border-radius: 16px;
                padding: 30px;
                backdrop-filter: blur(10px);
                margin-bottom: 30px;
            }
            h2 {
                font-size: 18px;
                margin-bottom: 20px;
                color: #fff;
            }
            table {
                width: 100%;
                border-collapse: collapse;
                margin-top: 10px;
            }
            th, td {
                padding: 14px;
                text-align: left;
                border-bottom: 1px solid var(--card-border);
            }
            th {
                color: var(--text-muted);
                font-weight: 600;
                font-size: 13px;
                text-transform: uppercase;
                letter-spacing: 0.5px;
            }
            td {
                font-size: 14px;
            }
            .state-id {
                font-family: monospace;
                color: var(--primary);
                background: rgba(240, 185, 11, 0.05);
                padding: 4px 8px;
                border-radius: 4px;
            }
            .no-data {
                text-align: center;
                color: var(--text-muted);
                padding: 40px 0;
            }
            footer {
                text-align: center;
                margin-top: auto;
                color: var(--text-muted);
                font-size: 12px;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <header>
                <h1>BiPass State Server</h1>
                <div class="status-badge">
                    <span class="status-dot"></span>
                    ONLINE
                </div>
            </header>

            <div class="card">
                <h2>Active Bot States</h2>
                {% if states_info %}
                <table>
                    <thead>
                        <tr>
                            <th>State ID</th>
                            <th>Last Updated</th>
                            <th>Size</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for s in states_info %}
                        <tr>
                            <td><span class="state-id">{{ s.id }}</span></td>
                            <td>{{ s.last_updated }}</td>
                            <td>{{ s.size }}</td>
                        </tr>
                        {% endfor %}
                    </tbody>
                </table>
                {% else %}
                <div class="no-data">No active states stored on this server yet.</div>
                {% endif %}
            </div>
            
            <footer>
                Powered by BiPass Local State Server
            </footer>
        </div>
    </body>
    </html>
    """
    return render_template_string(html_content, states_info=states_info)

@app.route("/v1/json", methods=["POST"])
def create_state():
    try:
        data = request.get_json(force=True)
    except Exception:
        data = {}
    
    state_id = str(uuid.uuid4())
    filepath = get_state_path(state_id)
    
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        # Build the exact URI as expected by JsonStorageManager
        base_url = request.url_root.rstrip("/")
        uri = f"{base_url}/v1/json/{state_id}"
        return jsonify({"uri": uri}), 200
    except Exception as e:
        return jsonify({"error": f"Failed to write state: {str(e)}"}), 500

@app.route("/v1/json/<path:item_id>", methods=["GET"])
def get_state(item_id):
    filepath = get_state_path(item_id)
    if not os.path.exists(filepath):
        return jsonify({"error": "State not found"}), 404
    
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        return jsonify(data), 200
    except Exception as e:
        return jsonify({"error": f"Failed to read state: {str(e)}"}), 500

@app.route("/v1/json/<path:item_id>", methods=["PUT"])
def update_state(item_id):
    filepath = get_state_path(item_id)
    try:
        data = request.get_json(force=True)
    except Exception:
        return jsonify({"error": "Invalid JSON"}), 400
        
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return "", 204
    except Exception as e:
        return jsonify({"error": f"Failed to update state: {str(e)}"}), 500

if __name__ == "__main__":
    # Default port for the state server is 10001
    app.run(host="0.0.0.0", port=10001)
