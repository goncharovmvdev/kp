import uvicorn
from fastapi import FastAPI, Depends
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session

app = FastAPI()


def get_db():
    db = sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=create_engine("postgresql://postgres:postgres@localhost:5432/postgres")
    )()
    try:
        yield db
    finally:
        db.close()


@app.get("/articles/{offset}/{limit}")
async def get_book_by_isbn(offset: int, limit: int, db: Session = Depends(get_db)):
    query = text(f"""
        SELECT data
        FROM scrapy_items
		OFFSET 350
        LIMIT 100
    """)
    return db.execute(query).fetchall()


if __name__ == "__main__":
    uvicorn.run("api:app", host="127.0.0.1", port=8000, reload=True)
