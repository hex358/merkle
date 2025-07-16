from sanic import text, Sanic, html
from sanic.exceptions import NotFound, ServerError

app = Sanic("test")

@app.route("/")
async def handler(request):
    return text("Hi 😎")

@app.exception(NotFound)
async def handle_404(request, exception):
    # or HTML:
    return html(
        "<html><body><h1>404 - Page Not Found</h1>"
        "<p>Sorry, the page you requested does not exist.</p></body></html>",
        status=404
    )

@app.exception(ServerError)
async def handle_502(request, exception):
    # or HTML:
    return html(
        "<html><body><h1>404 - Error 502</h1>"
        "<p>Sorry, server has crashed.</p></body></html>",
        status=404
    )
