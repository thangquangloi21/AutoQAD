from flask import Flask, jsonify, request
from datetime import datetime, timedelta
from WorkThread import WorkThread
from Log import Logger


app = Flask(__name__)
LAST_REQUEST_TIME = None
LAST_REQUEST_TIME2 = None
LAST_REQUEST_TIME3 = None
LIMIT = timedelta(minutes=30)
LIMIT2 = timedelta(minutes=30)
LIMIT3 = timedelta(minutes=30)
RUNING = False

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
    
    # nếu lỗi update error vào
    if not status:
        return jsonify({
            "error": "Export erorr",
        }), 429
    
    
    # TODO: refresh data + update SQL
    # nếu ok thì trả về done và update vào sql
    return jsonify({
        "status": "done",
        "time": now.strftime("%Y-%m-%d %H:%M:%S")
    })
    
    
    
@app.route("/test", methods=["POST"])
def test():
    now = datetime.now()
    # WorkThread.Insert_SQL(f"insert into ACWO.dbo.DataStatus (SYSTEM, TIME) values ('QAD', {now.strftime("%Y-%m-%d %H:%M:%S")})") 
    status = WorkThread.Insert_SQL(f"insert into ACWO.dbo.DataStatus (SYSTEM, TIME) values ('QAD','{now.strftime("%H:%M:%S %d-%m-%Y")}')")
    print(status)
    return jsonify({"message": f"Check status completed {status}"})
    


@app.route("/api/item", methods=["POST"])
def ExportItem():
    log.info("Update dữ liệu item")
    global LAST_REQUEST_TIME2
    now = datetime.now()
   
    
    
    
    if LAST_REQUEST_TIME2 and now - LAST_REQUEST_TIME2 < LIMIT2:
        wait = LIMIT2 - (now - LAST_REQUEST_TIME2)
        return jsonify({
            "error": "Too many requests",
            "retry_after_seconds": int(wait.total_seconds())
            }), 429

    LAST_REQUEST_TIME2 = now

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




@app.route("/api/wo", methods=["POST"])
def ExportWo():
    log.info("Update dữ liệu wo")
    global LAST_REQUEST_TIME3, RUNING
    now = datetime.now()
    CheckStatus = WorkThread.Check_Status("QAD")
     
    if CheckStatus != "error" and LAST_REQUEST_TIME3 and now - LAST_REQUEST_TIME3 < LIMIT3:
        wait = LIMIT3 - (now - LAST_REQUEST_TIME3)
        return jsonify({
            "error": "Too many requests",
            "retry_after_seconds": int(wait.total_seconds())
            }), 429

    LAST_REQUEST_TIME3 = now

    if RUNING:
        return jsonify({
            "error": "System is run"
            }), 429
    # update dữ liệu ở đây
    RUNING = True
    status = WorkThread.Export_WO()
        
    if not status:
        WorkThread.Insert_SQL(f"insert into ACWO.dbo.DataStatus (SYSTEM, TIME) values ('QAD','error')")
        RUNING = False
        return jsonify({
            "error": "Export erorr",
        }), 429
        
    # TODO: refresh data + update SQL
    now = datetime.now()
    
    RUNING = False
    WorkThread.Insert_SQL(f"insert into ACWO.dbo.DataStatus (SYSTEM, TIME) values ('QAD','{now.strftime("%H:%M %d-%m-%Y")}')")
    return jsonify({
        "status": "done",
        "time": now.strftime("%Y-%m-%d %H:%M:%S")
    })



# Start server
if __name__ == "__main__":
    log.info("Khởi động Flask API trên cổng 5555")
    app.run(
        host="0.0.0.0",  # cho phép máy khác truy cập
        port=5555,
        debug=True
    )
    log.info("Exit Flask API trên cổng 5555")
