const { performance } = require("perf_hooks");
const QueryStream = require("pg-query-stream");

module.exports = function (options = {}) {
  const pg = options.pg || require("pg");
  const name = options.name || "postgres";

  let pool;
  let config;
  let log;

  function stream(text, values, opts) {
    const query = new QueryStream(text, values, opts);
    return new Promise((resolve, reject) => {
      pool.connect((err, client, done) => {
        if (err) return reject(err);
        const stream = client.query(query);
        stream.on("end", done);
        resolve(stream);
      });
    });
  }

  async function query(text, values) {
    const query = typeof text === "string" ? { text, values } : text;
    return await pool.query(query).then((x) => x.rows);
  }

  async function loggingQuery(text, values) {
    const query = typeof text === "string" ? { text, values } : text;
    const logResults = query.logResults !== false;
    const start = performance.now();
    try {
      const results = await pool.query(query).then((x) => x.rows);
      const elapsed = performance.now() - start;
      const context = {
        query: { ...query, ...(logResults ? { results } : {}) },
      };
      log.info(context, "query took %dms", Math.ceil(elapsed));
      return results;
    } catch (err) {
      const elapsed = performance.now() - start;
      log.error(
        { query: { ...query, err } },
        "query took %dms",
        Math.ceil(elapsed),
      );
      throw err;
    }
  }

  function start(dependencies) {
    config = dependencies.config;
    log =
      (dependencies.logger && dependencies.logger.child({ component: name })) ||
      console;

    if (!config) throw new Error("config is required");
    if (!config.connectionString) {
      throw new Error("config.connectionString is required");
    }

    log.info(`Connecting to ${getConnectionUrl()}`);

    pool = new pg.Pool(config);

    pool.on("connect", async (client) => {
      client.on("notice", function (notice) {
        switch (notice.severity) {
          case "DEBUG": {
            log.debug(notice.message);
            break;
          }
          case "LOG": {
            log.info(notice.message);
            break;
          }
          case "INFO": {
            log.info(notice.message);
            break;
          }
          case "NOTICE": {
            log.info(notice.message);
            break;
          }
          case "WARNING": {
            log.warn(notice.message);
            break;
          }
          case "EXCEPTION": {
            log.error(notice.message);
            break;
          }
          default: {
            log.error(notice.message);
            break;
          }
        }
      });
      for (const query of config.onConnect || []) {
        try {
          await client.query(query);
        } catch (err) {
          log.error(`Error running query: ${query}`, err);
        }
      }
    });
    pool.on("error", function (err) {
      log.warn("An idle client has experienced an error", err);
    });

    return config.suppressQueryLogging !== true
      ? { query: loggingQuery, stream }
      : { query, stream };
  }

  async function stop() {
    if (!pool) return;
    log.info(`Disconnecting from ${getConnectionUrl()}`);
    await pool.end();
  }

  function getConnectionUrl() {
    const url = new URL(config.connectionString);
    return `postgres://${url.host || "localhost:5432"}${
      url.pathname || "/postgres"
    }`;
  }

  return { start, stop };
};
