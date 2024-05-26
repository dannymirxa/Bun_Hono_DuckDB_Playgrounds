import { Hono } from 'hono';
import DSRentryrouter from './routes/DSRentry';
import { cors } from 'hono/cors'

const app = new Hono()

Bun.serve({
    fetch: app.fetch,
    port: 3030
})

app.use("/", cors())
app.route("/", DSRentryrouter)