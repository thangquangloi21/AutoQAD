# class xử lý công việc trong thread riêng
import threading
import subprocess
import os
from Log import Logger
import paramiko
from sqlalchemy import create_engine, text


class WorkThread(threading.Thread):
    log = Logger()
    ssh = None
    host = 'tvc-sv15'
    usernamessh = 'mfg'
    passwordssh = 'tvcadmin'
    db_path = '/qad/prod/databases'
    db_name = 'tvcprod'
    remote_export_path = '/home/mfg/exp'
    # SQL
    server = "10.239.1.54"
    database = "Data_qad"
    schema = "dbo"
    username = "sa"
    password = "123456"
    def __init__(self):
        threading.Thread.__init__(self)
        # self.log.info("Application initialized")
        
    
    def Export_item(self):
         # tạo các dữ liệu cần thiết
        csv_path = r"Z:\exp\pt_mstr_export.csv"
        table = "pt_mstr"
        try:
            # xuất dữ liệu ra csv
            self.log.info("Xuất dữ liệu inventory ra file CSV")
            self.ExportItem()
            self.log.info(f"import vào db")
            # gọi hàm import vào db
            self.log.info("Kiểm tra và xóa dữ liệu")
            self.check_table_data(table)
            self.import_csv_to_sql_server(csv_path, self.server, self.database, self.schema, table, self.username, self.password)
            self.delete_file(csv_path)
            return True
        except Exception as e:
            return False

    def Export_inventory(self):
        # tạo các dữ liệu cần thiết
        csv_path = r"Z:\exp\ld_det_export.csv"
        table = "ld_det"
        try:
            # xuất dữ liệu ra csv
            self.log.info("Xuất dữ liệu inventory ra file CSV")
            self.ExportInventory()
            self.log.info(f"import vào db")
            # gọi hàm import vào db
            self.check_table_data(table)
            self.import_csv_to_sql_server(csv_path, self.server, self.database, self.schema, table, self.username, self.password)
            self.delete_file(csv_path)
            return True
        except Exception as e:
            return False

    def delete_file(self, file_path):
        if os.path.exists(file_path):
            os.remove(file_path)
            self.log.info(f"Tệp {file_path} đã được xóa.")
        else:
           self.log.error("Tệp không tồn tại.")
    
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
                self.log.warning(f"Cảnh báo/Thông tin:\n" + f"{result.stderr}")

        except subprocess.CalledProcessError as e:
            self.log.error("LỖI BCP – Chi tiết đầy đủ:")
            self.log.error(f"Return code:", f"{e.returncode}")
            self.log.error(f"STDOUT:\n", f"{e.stdout}")
            self.log.error(f"STDERR (lỗi chính):\n"+ f"{e.stderr}")
            self.log.error("\nKiểm tra file error: bcp_error_rows.log để xem dòng nào lỗi.")
            self.log.error("Các lỗi thường gặp:")
            self.log.error("- Nếu 'Invalid character value': dữ liệu không khớp kiểu cột (ví dụ qty là số nhưng có chữ)")
            self.log.error("- Nếu 'Unexpected EOF' hoặc 'Text incomplete': thử thay -r 0x0a thành -r \"\\n\" hoặc kiểm tra line ending file")
            self.log.error("- Nếu 'Unable to open data-file': quyền truy cập file Z: hoặc path sai")

        except FileNotFoundError:
            self.log.error("Không tìm thấy bcp.exe! Hãy kiểm tra:")
            self.log.error("1. Cài SQL Server Command Line Utilities (tải từ Microsoft)")
            self.log.error("2. Thêm đường dẫn bcp.exe vào PATH (thường ở C:\\Program Files\\Microsoft SQL Server\\Client SDK\\ODBC\\170\\Tools\\Binn)")


    def ExportItem(self):
        """Export pt_mstr table"""
        create_script = f'''
cat > /tmp/export.p << 'EOF'
OUTPUT TO "/home/mfg/exp/pt_mstr_export.csv".
PUT UNFORMATTED "pt_part;pt_um;pt_prod_line;pt_part_type" SKIP.
FOR EACH pt_mstr NO-LOCK:
PUT UNFORMATTED 
STRING(pt_mstr.pt_part) + ";" + 
STRING(pt_mstr.pt_um) + ";" + 
STRING(pt_mstr.pt_prod_line) + ";" + 
STRING(pt_mstr.pt_part_type) SKIP.
END.
OUTPUT CLOSE.
EOF
        '''
        self.exportdata(create_script, "pt_mstr_export.csv") 
    def ExportInventory(self):
          # today = datetime.now()
        # yesterday = today - timedelta(days=1)
        # yesterday = yesterday.strftime("%d/%m/%y")
        
        """Export pt_mstr table"""
        create_script = f'''
cat > /tmp/export.p << 'EOF'
OUTPUT TO "/home/mfg/exp/ld_det_export.csv".
PUT UNFORMATTED "ld_site;ld_loc;ld_part;ld_lot;ld_status;ld_qty_all;ld_qty_oh;ld_ref" SKIP.
FOR EACH ld_det NO-LOCK WHERE ld_det.ld_qty_oh > 0:
PUT UNFORMATTED 
STRING(ld_det.ld_site) + ";" + 
STRING(ld_det.ld_loc) + ";" + 
STRING(ld_det.ld_part) + ";" + 
STRING(ld_det.ld_lot) + ";" + 
STRING(ld_det.ld_status) + ";" + 
STRING(ld_det.ld_qty_all) + ";" +
STRING(ld_det.ld_qty_oh) + ";" +
STRING(ld_det.ld_ref) SKIP.
END.
OUTPUT CLOSE.

EOF
        '''
        self.exportdata(create_script, "ld_det_export.csv") 
    
    
    
    def exportdata(self, script, filename):
        """Export data from QAD database"""
        try:
            self.connect()
            
            # Create export script
            self.log.info(f"Creating script for {filename}...")
            stdout, stderr = self.exec_command(script)
            if stderr:
                self.log.info(f"Script creation error: {stderr}")
            
            # Run Progress
            self.log.info(f"Running Progress export...")
            cmd = f'''
export DLC=/qad/oe117
cd {self.db_path}
$DLC/bin/_progres -b -p /tmp/export.p -db {self.db_name}
            '''
            stdout, stderr = self.exec_command(cmd)
            if stderr:
                self.log.info(f"Progress error: {stderr}")
            
            
            # # Download file
            # self.log.info(f"Downloading {filename}...")
            # sftp = self.ssh.open_sftp()
            # remote_file = f'{self.remote_export_path}/{filename}'
            # local_file = os.path.join(self.local_export_path, filename)
            # sftp.get(remote_file, local_file)
            # sftp.close()
            
            # self.log.info(f"✓ {filename} downloaded successfully to {local_file}")
            self.log.info("xuất ok")
            
        except Exception as e:
            self.log.info(f"✗ Export Error: {e}")
        finally:
            self.disconnect()
            
    def exec_command(self, cmd):
        """Execute SSH command and return output"""
        stdin, stdout, stderr = self.ssh.exec_command(cmd)
        return stdout.read().decode(), stderr.read().decode()
    

    def disconnect(self):
        """Disconnect SSH"""
        if self.ssh:
            self.ssh.close()
            
    def connect(self):
        """Connect SSH"""
        try:
            self.ssh = paramiko.SSHClient()
            self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh.connect(self.host, username=self.usernamessh, password=self.passwordssh)
            self.log.info(f"✓ Connected to {self.host}")
        except Exception as e:
            self.log.info(f"✗ SSH Connection Error: {e}")
            raise
        
    def check_table_data(self, table):
        try:
            conect = self.conn()
            # if data cout > 0 delete table
            with conect.begin() as connection:
                result = connection.execute(text(f"SELECT COUNT(*) FROM [{table}]"))
                row_count = result.scalar()
                if row_count > 0:
                    self.log.info("Có dữ liệu trong bảng.")
                    self.log.info(f"The table '{table}' has {row_count} rows.")
                    self.log.info(f"DELETE FROM [{table}]")
                    with conect.begin() as connection:
                        connection.execute(text(f"DELETE FROM [{table}]") )
                        return
                else:
                    self.log.info(f"The table '{table}' is empty.")
                    self.log.info("Không có dữ liệu trong bảng")
                    return
        except Exception as e:
            self.log.error(f"error: {e}")
            
    def conn(self):
        # Database connection and data processing
        try:
            server = self.server
            database = self.database
            username = self.username
            password = self.password
            driver = 'ODBC Driver 18 for SQL Server'

            connection_string = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}&TrustServerCertificate=yes'
            engine = create_engine(connection_string)
            self.log.info("connect success")
            return engine
        except Exception as e:
            self.log.error(f"Error connecting to database: {e}")