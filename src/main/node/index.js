const fs = require("node:fs");
const fsp = require("node:fs/promises");
const os = require("node:os");
const workerThreads = require("node:worker_threads");
const { performance } = require("node:perf_hooks");

const config = {
  MAX_LINE_LENGTH: 106,
  STATION_NAME_LENGTH: 100,
  TEMPERATURE_LENGTH: 5,
  TOKEN_STATION_NAME: 0,
  TOKEN_TEMPERATURE_VALUE: 1,
  CHAR_SEMICOLON: ";".charCodeAt(0),
  NEWLINE_FEED: "\n".charCodeAt(0),
  CHAR_MINUS: "-".charCodeAt(0),
};

/**
 * @typedef {Map<string, {min: number, max: number, sum: number, count: number}>} ResultMap
 */

async function spawnWorkers() {
  const filename = process.argv[2];
  if (!filename) throw new Error("Filename not provided");
  console.log("Reading file ", filename);
  const file = await fsp.open(filename);
  const size = (await file.stat()).size;
  const numThreads = os.cpus().length;
  const chunkSize = Math.floor(size / numThreads);
  console.log("Chunks of = ", chunkSize);

  /** @type {number[]} */
  const chunks = [];
  const buffer = Buffer.alloc(config.MAX_LINE_LENGTH);
  let readFileFromOffset = 0;

  while (true) {
    readFileFromOffset += chunkSize;
    if (readFileFromOffset >= size) {
      chunks.push(size);
      break;
    }

    await file.read(buffer, 0, config.MAX_LINE_LENGTH, readFileFromOffset);
    const newlineChar = buffer.indexOf(10);
    buffer.fill(0);

    if (newlineChar === -1) {
      chunks.push(size);
    } else {
      readFileFromOffset += newlineChar + 1;
      chunks.push(readFileFromOffset);
    }
  }

  await file.close();

  /** @type {ResultMap} */
  const finalResults = new Map();
  let stoppedWorkers = 0;
  const timeMap = new Map();

  for (let idx = 0; idx < chunks.length; idx++) {
    const startTime = performance.now();
    const worker = new workerThreads.Worker("./src/main/node/index.js", {
      workerData: {
        filename,
        start: idx === 0 ? 0 : chunks[idx - 1],
        end: chunks[idx],
      },
    });

    const wid = worker.threadId;
    timeMap.set(wid, startTime);
    worker.on("message", (/** @type {ResultMap} */ data) => {
      for (const [key, value] of data.entries()) {
        const existing = finalResults.get(key);
        if (existing) {
          existing.min = Math.min(existing.min, value.min);
          existing.max = Math.max(existing.max, value.max);
          existing.count += 1;
          existing.sum += value.sum;
        } else {
          finalResults.set(key, value);
        }
      }
    });

    worker.on("error", (err) => {
      console.error("Recieved error from worker", err);
    });

    worker.on("exit", () => {
      console.log(
        `Worker ${wid} finished the task successfully in ${Math.round(
          performance.now() - timeMap.get(wid)
        )}ms`
      );
      stoppedWorkers += 1;

      if (stoppedWorkers === chunks.length) {
        printResults(finalResults);
      }
    });
  }
}

function workerTaskExecutor() {
  const { filename, start, end } = workerThreads.workerData;
  if (start >= end) {
    workerThreads.parentPort.postMessage(new Map());
  } else {
    const readStream = fs.createReadStream(filename, {
      start,
      end: end - 1,
    });

    parseStream(readStream);
  }
}

async function solve() {
  if (workerThreads.isMainThread) {
    await spawnWorkers();
  } else {
    workerTaskExecutor();
  }
}

/**
 *
 * @param {ResultMap} finalResults
 */
function printResults(finalResults) {
  const sortedStations = Array.from(finalResults.keys()).sort();
  sortedStations.forEach((station) => console.log(station));
  // process.stdout.write("{");
  // for (let idx = 0; idx < sortedStations.length; idx++) {
  //   if (idx > 0) {
  //     process.stdout.write(", ");
  //   }
  //   const stationResult = finalResults.get(sortedStations[idx]);
  //   process.stdout.write(`${sortedStations[idx]}=`);
  //   process.stdout.write(`${roundWithOneDecimal(stationResult.min / 10)}/`);
  //   process.stdout.write(
  //     `${roundWithOneDecimal(stationResult.sum / (10 * stationResult.count))}/`
  //   );
  //   process.stdout.write(`${roundWithOneDecimal(stationResult.max / 10)}`);
  // }
  // process.stdout.write("}\n");
}

/**
 *
 * @param {fs.ReadStream} readStream
 */
function parseStream(readStream) {
  /** @type {ResultMap} */
  const chunkMap = new Map();

  readStream.on("data", (/** @type {Buffer} */ chunk) => {
    parseChunk(chunk, chunkMap);
  });

  readStream.on("end", () => {
    workerThreads.parentPort.postMessage(chunkMap);
  });
}

/**
 * @param {Buffer} chunk
 * @param {ResultMap} chunkMap
 */
function parseChunk(chunk, chunkMap) {
  let readingToken = config.TOKEN_STATION_NAME;
  let stationName = Buffer.allocUnsafe(config.STATION_NAME_LENGTH);
  let stationNameLen = 0;
  let temperature = Buffer.allocUnsafe(config.TEMPERATURE_LENGTH);
  let temperatureLen = 0;
  for (let idx = 0; idx < chunk.length; idx++) {
    if (chunk[idx] === config.CHAR_SEMICOLON) {
      readingToken = config.TOKEN_TEMPERATURE_VALUE;
    } else if (chunk[idx] === config.NEWLINE_FEED) {
      const stationNameStr = stationName.toString("utf-8", 0, stationNameLen);
      let temperatureFloat = 0 / 0;
      try {
        temperatureFloat = parseFloatBufferIntoInt(temperature, temperatureLen);
      } catch (err) {
        console.error("Error parsing temperature: ", err);
      }
      const existing = chunkMap.get(stationNameStr);
      if (existing) {
        existing.min = Math.min(existing.min, temperatureFloat);
        existing.max = Math.max(existing.max, temperatureFloat);
        existing.sum += temperatureFloat;
        existing.count += 1;
      } else {
        chunkMap.set(stationNameStr, {
          min: temperatureFloat,
          max: temperatureFloat,
          sum: temperatureFloat,
          count: 1,
        });
      }

      readingToken = config.TOKEN_STATION_NAME;
      stationNameLen = 0;
      temperatureLen = 0;
    } else if (readingToken === config.TOKEN_STATION_NAME) {
      stationName[stationNameLen] = chunk[idx];
      stationNameLen += 1;
    } else {
      temperature[temperatureLen] = chunk[idx];
      temperatureLen += 1;
    }
  }
}

/**
 *
 * @param {Buffer} buffer
 * @param {number} length
 */
function parseFloatBufferIntoInt(buffer, length) {
  if (buffer[0] === config.CHAR_MINUS) {
    if (length === 4) {
      return -(parseDigit(buffer[1]) * 10 + parseDigit(buffer[3]));
    } else if (length === 5) {
      return -(
        parseDigit(buffer[1]) * 100 +
        parseDigit(buffer[2]) * 10 +
        parseDigit(buffer[4])
      );
    }
  } else {
    if (length === 3) {
      return parseDigit(buffer[0]) * 10 + parseDigit(buffer[2]);
    } else if (length === 4) {
      return (
        parseDigit(buffer[0]) * 100 +
        parseDigit(buffer[1]) * 10 +
        parseDigit(buffer[3])
      );
    }
  }
}

/**
 * The expression char - 0x30 is a common way to convert a character representing a digit in ASCII to its corresponding numeric value.
 * @param {number} char
 * @returns
 */
function parseDigit(char) {
  return char - 0x30;
}

/**
 * @example
 * round(1.2345) // "1.2"
 * round(1.55) // "1.6"
 * round(1) // "1.0"
 *
 * @param {number} num
 * @returns {string}
 */
function roundWithOneDecimal(num) {
  const fixed = Math.round(10 * num) / 10;
  return fixed.toFixed(1);
}

solve();
