from fastapi import FastAPI

app = FastAPI(title="Minha Regiao API")


@app.get("/")
def hello_world() -> dict[str, str]:
    return {"message": "Hello, Minha Regiao!"}
