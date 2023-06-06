anatomic-pg
===========

An [anatomic](https://github.com/add1ed/anatomic) component for [pg](https://github.com/brianc/node-postgres).

## tl;dr

```js
const anatomic = require("anatomic");
const pg = require("anatomic-pg");

async function main() {
  const system = anatomic()
    .add('postgres', pg()).dependsOn("config")
    .configure({ postgres: { connectionString: 'postgres://localhost:5432' } });

  const { postgres } = await system.start();
  console.log(await postgres.query("SELECT version();"));
  await system.stop();
}

main();
```