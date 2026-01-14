# class xử lý công việc trong thread riêng
import threading
import subprocess
import os
from Log import Logger

class WorkThread(threading.Thread):
    log = Logger()
    def __init__(self):
        threading.Thread.__init__(self)
        



    def Export_inventory(self):
        # tạo các dữ liệu cần thiết
        csv_path = r"D:\ld_det_export.csv"
        server = "pc-tql"
        database = "Data_qad"
        schema = "dbo"
        table = "ld_det"
        username = "sa"
        password = "P@ssw0rd2025!"
        # xuất dữ liệu ra csv
        
        self.log.info("Xuất dữ liệu inventory ra file CSV")
        self.log.info(f"import vào db")
        # gọi hàm import vào db
        self.import_csv_to_sql_server(csv_path, server, database, schema, table, username, password)

    def import_csv_to_sql_server(self, csv_path, server, database, schema, table, username, password):
            # Kiểm tra file tồn tại trước
        if not os.path.exists(csv_path):
            self.log.info(f"File không tồn tại: {csv_path}")
            exit(1)

        # Command bcp với các tùy chọn an toàn nhất cho CSV ; delimited + header
        bcp_cmd = [
            "bcp",
            f"{database}.{schema}.{table}",
            "in",
            csv_path,
            "-S", server,
            "-U", username,
            "-P", password,
            "-c",                     # character mode (an toàn cho text)
            "-t", ";",                # field terminator = ;
            "-r", "0x0a",             # row terminator = \n (hex cho Unix LF, tránh lỗi \r\n)
            "-F", "2",                # Bắt đầu từ dòng 2 (bỏ header)
            "-b", "5000",             # batch size nhỏ để giảm lock/log, commit thường xuyên
            "-a", "65535",           # packet size lớn nhất (tăng tốc độ truyền)
            "-e", "bcp_error_rows.log",  # log các dòng lỗi (rất quan trọng!)
            "-m", "100"               # max errors = 100 (cho phép vài lỗi nhỏ mà vẫn chạy tiếp)
        ]

        self.log.info("Đang chạy lệnh bcp:")
        self.log.info(" ".join(bcp_cmd))  # In lệnh để debug

        try:
            result = subprocess.run(
                bcp_cmd,
                check=True,
                capture_output=True,
                text=True,
                encoding='utf-8',       # xử lý UTF-8 tốt hơn
                errors='replace'
            )
            self.log.info(f"Import THÀNH CÔNG!")
            self.log.info(f"Output từ bcp:\n" + f"{result.stdout}")
            if result.stderr:
                self.log.info(f"Cảnh báo/Thông tin:\n" + f"{result.stderr}")

        except subprocess.CalledProcessError as e:
            self.log.info("LỖI BCP – Chi tiết đầy đủ:")
            self.log.info(f"Return code:", f"{e.returncode}")
            self.log.info(f"STDOUT:\n", f"{e.stdout}")
            self.log.info(f"STDERR (lỗi chính):\n"+ f"{e.stderr}")
            self.log.info("\nKiểm tra file error: bcp_error_rows.log để xem dòng nào lỗi.")
            self.log.info("Các lỗi thường gặp:")
            self.log.info("- Nếu 'Invalid character value': dữ liệu không khớp kiểu cột (ví dụ qty là số nhưng có chữ)")
            self.log.info("- Nếu 'Unexpected EOF' hoặc 'Text incomplete': thử thay -r 0x0a thành -r \"\\n\" hoặc kiểm tra line ending file")
            self.log.info("- Nếu 'Unable to open data-file': quyền truy cập file Z: hoặc path sai")

        except FileNotFoundError:
            self.log.info("Không tìm thấy bcp.exe! Hãy kiểm tra:")
            self.log.info("1. Cài SQL Server Command Line Utilities (tải từ Microsoft)")
            self.log.info("2. Thêm đường dẫn bcp.exe vào PATH (thường ở C:\\Program Files\\Microsoft SQL Server\\Client SDK\\ODBC\\170\\Tools\\Binn)")


