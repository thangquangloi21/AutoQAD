from flask import Flask, jsonify, request
from datetime import datetime, timedelta
from WorkThread import WorkThread
from Log import Logger


app = Flask(__name__)
LAST_REQUEST_TIME = None
LIMIT = timedelta(minutes=30)
log = Logger()
WorkThread = WorkThread()


# Health check
@app.route("/", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "service": "Flask API",
        "version": "1.0"
    })

# GET example
@app.route("/api/hello", methods=["GET"])
def hello():
    name = request.args.get("name", "World")
    return jsonify({
        "message": f"Hello {name}"
    })

# POST example
@app.route("/api/echo", methods=["POST"])
def echo():
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"error": "No JSON body"}), 400

    return jsonify({
        "received": data
    })

@app.route("/api/inventory", methods=["POST"])
def item():
    log.info("Update dữ liệu inventory")
    global LAST_REQUEST_TIME
    now = datetime.now()
    if LAST_REQUEST_TIME and now - LAST_REQUEST_TIME < LIMIT:
        wait = LIMIT - (now - LAST_REQUEST_TIME)
        return jsonify({
            "error": "Too many requests",
            "retry_after_seconds": int(wait.total_seconds())
        }), 429

    LAST_REQUEST_TIME = now

    # update dữ liệu ở đây
    status = WorkThread.Export_inventory()
    
    if not status:
        return jsonify({
            "error": "Export erorr",
            "retry_after_seconds": int(wait.total_seconds())
        }), 429
    
    # TODO: refresh data + update SQL
    return jsonify({
        "status": "done",
        "time": now.strftime("%Y-%m-%d %H:%M:%S")
    })
    
@app.route("/api/item", methods=["POST"])
def ExportItem():
    log.info("Update dữ liệu item")
    global LAST_REQUEST_TIME
    now = datetime.now()

    if LAST_REQUEST_TIME and now - LAST_REQUEST_TIME < LIMIT:
        wait = LIMIT - (now - LAST_REQUEST_TIME)
        return jsonify({
            "error": "Too many requests",
            "retry_after_seconds": int(wait.total_seconds())
            }), 429

    LAST_REQUEST_TIME = now

    # update dữ liệu ở đây
    status = WorkThread.Export_item()
        
    if not status:
        return jsonify({
            "error": "Export erorr",
        }), 429
        
    # TODO: refresh data + update SQL
    return jsonify({
        "status": "done",
        "time": now.strftime("%Y-%m-%d %H:%M:%S")
    })

# Start server
if __name__ == "__main__":
    log.info("Khởi động Flask API trên cổng 5000")
    app.run(
        host="0.0.0.0",  # cho phép máy khác truy cập
        port=5000,
        debug=True
    )
    log.info("Exit Flask API trên cổng 5000")
