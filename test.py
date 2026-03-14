# -*- coding: utf-8 -*-
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from urllib.parse import quote_plus
from Log import Logger  # giữ nguyên nếu bạn đã có module này
from datetime import datetime, timedelta


class DbClient:
    def __init__(self, server: str, database: str, username: str, password: str, driver: str = "ODBC Driver 17 for SQL Server"):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.driver = driver
        self.log = Logger()
        self.engine: Engine | None = None

    def conn1(self) -> Engine:
        """Khởi tạo SQLAlchemy Engine (kết nối lười, chỉ mở khi execute)."""
        try:
            # Quan trọng: URL-encode driver và các tham số có khoảng trắng/ký tự đặc biệt
            driver_enc = quote_plus(self.driver)
            # Có thể thêm Encrypt/TrustServerCertificate để tránh lỗi chứng chỉ với ODBC 18
            # Lưu ý: Chỉ bật TrustServerCertificate khi bạn hiểu rủi ro bảo mật.
            params = "Encrypt=no;TrustServerCertificate=yes"
            params_enc = quote_plus(params)

            conn1ection_uri = (
                f"mssql+pyodbc://{quote_plus(self.username)}:{quote_plus(self.password)}"
                f"@{self.server}/{quote_plus(self.database)}"
                f"?driver={driver_enc}&{params_enc}"
            )

            # Tùy chọn pool_pre_ping giúp phát hiện kết nối chết.
            self.engine = create_engine(
                conn1ection_uri,
                pool_pre_ping=True,
                # fast_executemany chỉ áp dụng khi bạn dùng executemany với pyodbc cursor,
                # nhưng vẫn ổn để bật qua event nếu có nhu cầu bulk insert.
                # future=True  # nếu muốn sử dụng API 2.0
            )
            self.log.info("Kết nối engine tạo thành công.")
            return self.engine
        except Exception as e:
            self.log.error(f"Lỗi tạo engine: {e}")
            raise


    def execute_sql(self, sql: str, params: dict = None) -> any:
        """
        Thực thi câu lệnh SQL tùy ý.
        - Đối với SELECT: trả về danh sách các hàng (list of tuples).
        - Đối với UPDATE/INSERT/DELETE: trả về số hàng bị ảnh hưởng.
        - Sử dụng params để tránh SQL injection.
        """
        try:
            engine = self.engine or self.conn1()
            with engine.conn1ect() as conn1ection:
                result = conn1ection.execute(text(sql), params or {})
                if sql.strip().upper().startswith('SELECT'):
                    rows = result.fetchall()
                    self.log.info(f"Truy vấn SELECT trả về {len(rows)} hàng.")
                    return rows
                else:
                    conn1ection.commit()
                    row_count = result.rowcount
                    self.log.info(f"Lệnh thực thi ảnh hưởng {row_count} hàng.")
                    return row_count
        except Exception as e:
            self.log.error(f"Lỗi khi thực thi SQL: {e}")
            raise

if __name__ == "__main__":
    # --- Cấu hình của bạn ---
    server = "10.239.1.54"
    database = "Data_qad"
    schema = "dbo"
    username = "sa"
    password = "123456"

    db = DbClient(server, database, username, password)
    
    
    # Ví dụ truy vấn SELECT
    now = datetime.now()
    # rows = db.execute_sql(f"insert into ACWO.dbo.DataStatus (SYSTEM, TIME) values ('SAP', '{now.strftime("%H:%M %d-%m-%Y")}')")
    
    
    rows = db.execute_sql("SELECT TOP (1) [ID] ,[SYSTEM] ,[TIME] FROM [ACWO].[dbo].[DataStatus]  where system = 'QAD' order by id desc")

    print(f"Kết quả SELECT: {rows[0][2]}")
    