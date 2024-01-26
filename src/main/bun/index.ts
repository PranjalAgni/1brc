import fsp from "node:fs/promises";
import fs from "node:fs";
import os from "node:os";
import util from "node:util";
import { Worker } from "node:worker_threads";

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

const fsRead: (
  fd: number,
  buffer: Uint8Array,
  offset: number,
  length: number,
  position: number
) => Promise<number> = util.promisify(fs.read) as any;

const fsClose: (fd: number) => Promise<void> = util.promisify(fs.close);

type CalcResultsCont = Map<
  string,
  { min: number; max: number; sum: number; count: number }
>;

async function findChunkOffset(file: fsp.FileHandle) {
  const size = (await file.stat()).size;
  const totalThreads = os.cpus().length;
  const chunkSize = 1 || Math.floor(size / totalThreads);
  let offset = 0;
  const bufFindNl = Buffer.alloc(config.MAX_LINE_LENGTH);
  const chunkOffsets: number[] = [];
  while (true) {
    offset += chunkSize;

    if (offset >= size) {
      chunkOffsets.push(size);
      break;
    }

    await fsRead(file.fd, bufFindNl, 0, config.MAX_LINE_LENGTH, offset);

    const nlPos = bufFindNl.indexOf(10);
    bufFindNl.fill(0);

    if (nlPos === -1) {
      chunkOffsets.push(size);
      break;
    } else {
      offset += nlPos + 1;
      chunkOffsets.push(offset);
    }
  }

  await fsClose(file.fd);

  return chunkOffsets;
}

function distributeTask(filename: string, chunkOffsets: number[]) {
  const compiledResults: CalcResultsCont = new Map();
  let completedWorkers = 0;
  for (let idx = 0; idx < chunkOffsets.length; idx++) {
    const worker = new Worker("./src/main/bun/workers.ts");
    worker.postMessage({
      filename,
      start: idx === 0 ? 0 : chunkOffsets[idx - 1],
      end: chunkOffsets[idx],
    });

    worker.addListener("messageerror", (event) => {
      console.error(event);
    });

    worker.addListener("open", (event) => {
      // console.error("Worker started");
    });

    worker.addListener("error", (err) => {
      console.error(err);
    });

    worker.addListener("close", (event) => {
      console.error("Worker stopped");
    });

    worker.addListener("message", (event): void => {
      const message = event.data as CalcResultsCont;

      console.log(`Got map from worker: ${message.size}`);

      worker.unref();

      for (let [key, value] of message.entries()) {
        const existing = compiledResults.get(key);
        if (existing) {
          existing.min = Math.min(existing.min, value.min);
          existing.max = Math.max(existing.max, value.max);
          existing.sum += value.sum;
          existing.count += value.count;
        } else {
          compiledResults.set(key, value);
        }
      }

      completedWorkers++;
      if (completedWorkers === chunkOffsets.length) {
        printCompiledResults(compiledResults);
      }
    });
  }
}
async function solve() {
  const filename = process.argv[2];
  if (!filename) throw new Error("Filename not provided");
  const file = await fsp.open(filename);
  const chunkOffsets = await findChunkOffset(file);
  distributeTask(filename, chunkOffsets);
}

/**
 * @param {CalcResultsCont} compiledResults
 */
function printCompiledResults(compiledResults: CalcResultsCont) {
  const sortedStations = Array.from(compiledResults.keys()).sort();

  process.stdout.write("{");
  for (let i = 0; i < sortedStations.length; i++) {
    if (i > 0) {
      process.stdout.write(", ");
    }
    const data = compiledResults.get(sortedStations[i])!;
    process.stdout.write(sortedStations[i]);
    process.stdout.write("=");
    process.stdout.write(
      round(data.min / 10) +
        "/" +
        round(data.sum / 10 / data.count) +
        "/" +
        round(data.max / 10)
    );
  }
  process.stdout.write("}\n");
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
function round(num: number) {
  const fixed = Math.round(10 * num) / 10;

  return fixed.toFixed(1);
}

solve();
