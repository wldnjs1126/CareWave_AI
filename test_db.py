from sqlalchemy import text

from database.connection import engine


try:
    with engine.connect() as conn:

        result = conn.execute(
            text("SELECT NOW();")
        )

        print("DB 연결 성공")
        print("DB 시간:", result.scalar())

except Exception as e:

    print("DB 연결 실패")
    print(e)