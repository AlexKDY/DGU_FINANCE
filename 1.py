
import pymysql

# MySQL 데이터베이스 연결
db = pymysql.connect(
    host="localhost",        # MySQL 서버 호스트
    user="root",             # MySQL 사용자
    password="rlawjdgus1!",# 비밀번호
    database="mysql" # 사용할 데이터베이스
)

cursor = db.cursor()


# 연결 닫기
cursor.close()
db.close()
