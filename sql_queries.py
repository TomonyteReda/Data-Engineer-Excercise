query_1 = """CREATE TABLE IF NOT EXISTS activities (datetime DATETIME, impression_count BIGINT,
                        click_count BIGINT, audit_loaded_datetime DATETIME);"""

query_2 = "SELECT * FROM activities"