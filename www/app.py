from sanic import Sanic
from sanic.response import json, text, redirect
from sanic import Blueprint

app = Sanic()

'''
@app.route("/")
async def test(requret):
    return json({"hello": "world"})

@app.route("/number/<integer_arg:int>")
async def integer_handler(requret, integer_arg):
    return text("Interger - {}".format(integer_arg))


@app.route("number/<number_arg:number>")
async def number_hander(requret, number_arg):
    return text("数字 - {}".format(number_arg))


@app.route('/person/<name:[A-z]+>')
async def person_handler(request, name):
    return text('Person - {}'.format(name))


@app.route('/folder/<folder_id:[A-z0-9]{0,4}>')
async def folder_handler(request, folder_id):
    return text('Folder - {}'.format(folder_id))


@app.route('/post', methods=['POST'])
async def post_handler(request):
    return text('POST request - {}'.format(request.json))


@app.route('/get', methods=['GET'])
async def get_handler(request):
    return text('GET request - {}'.format(request.args))

@app.post('/post')
async def post_handler(request):
    return text('POST request - {}'.format(request.json))


@app.get('/get')
async def get_handler(request):
    return text('GET request - {}'.format(request.args))

# Define the handler functions


async def handler1(request):
    return text('OK')


async def handler2(request, name):
    return text('Folder - {}'.format(name))


async def person_handler2(request, name):
    return text('Person - {}'.format(name))

# Add each handler function as a route
app.add_route(handler1, '/test')
app.add_route(handler2, '/folder/<name>')
app.add_route(person_handler2, '/person/<name:[A-z]>', methods=['GET'])
'''

'''
@app.route('/')
async def index(request):
    # generate a URL for the endpoint `post_handler`
    url = app.url_for('post_handler', post_id=5)
    # the URL is `/posts/5`, redirect to it
    return redirect(url)


@app.route('/posts/<post_id>')
async def post_handler(request, post_id):
    return text('Post - {}'.format(post_id))
'''

from my_blueprint import bp

app = Sanic(__name__)
app.blueprint(bp)


if __name__ == "__main__":
    app.run("127.0.0.1", port=9000)
