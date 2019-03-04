from sanic import Sanic
from sanic.response import json

app = Sanic()


@app.route("/")
async def test(requret):
    return json({"hello": "world"})

if __name__ == "__main__":
    app.run("127.0.0.1", port=9000)
