import * as fs from "node:fs";

const config = {
  STATION_NAME_LENGTH: 100,
  TEMPERATURE_LENGTH: 5,
  TOKEN_STATION_NAME: 0,
  TOKEN_TEMPERATURE_VALUE: 1,
  CHAR_SEMICOLON: ";".charCodeAt(0),
  NEWLINE_FEED: "\n".charCodeAt(0),
  CHAR_MINUS: "-".charCodeAt(0),
};

declare var self: Worker;

self.addEventListener("message", (event) => {
  worker(event.data.filename, event.data.start, event.data.end);
});

function worker(fileName: string, start: number, end: number) {
  if (start > end - 1) {
    postMessage(new Map());
  } else {
    const readStream = fs.createReadStream(fileName, {
      start: start,
      end: end - 1,
    });

    parseStream(readStream);
  }
}

/**
 * @param {import('node:fs').ReadStream} readStream
 */
function parseStream(readStream: fs.ReadStream) {
  let readingToken = config.TOKEN_STATION_NAME;

  let stationName = Buffer.allocUnsafe(100);
  let stationNameLen = 0;

  let temperature = Buffer.allocUnsafe(5);
  let temperatureLen = 0;

  let rowCount = 0;

  /**
   * @type {CalcResultsCont}
   */
  const map = new Map();

  /**
   * @param {Buffer} chunk
   * @returns {void}
   */
  function parseChunk(chunk: Buffer) {
    for (let i = 0; i < chunk.length; i++) {
      if (chunk[i] === config.CHAR_SEMICOLON) {
        readingToken = config.TOKEN_TEMPERATURE_VALUE;
      } else if (chunk[i] === config.NEWLINE_FEED) {
        const stationNameStr = stationName.toString("utf8", 0, stationNameLen);

        let temperatureFloat = 0 | 0;
        try {
          temperatureFloat = parseFloatBufferIntoInt(
            temperature,
            temperatureLen
          ) as number;
        } catch (err: any) {
          console.log({ temperature, temperatureLen }, err.message);
          throw err;
        }

        const existing = map.get(stationNameStr);

        if (existing) {
          existing.min =
            existing.min < temperatureFloat ? existing.min : temperatureFloat;
          existing.max =
            existing.max > temperatureFloat ? existing.max : temperatureFloat;
          existing.sum += temperatureFloat;
          existing.count++;
        } else {
          map.set(stationNameStr, {
            min: temperatureFloat,
            max: temperatureFloat,
            sum: temperatureFloat,
            count: 1,
          });
        }

        readingToken = config.TOKEN_STATION_NAME;
        stationNameLen = 0;
        temperatureLen = 0;
        rowCount++;
      } else if (readingToken === config.TOKEN_STATION_NAME) {
        stationName[stationNameLen] = chunk[i];
        stationNameLen++;
      } else {
        temperature[temperatureLen] = chunk[i];
        temperatureLen++;
      }
    }
  }

  readStream.on("data", (chunk: Buffer) => {
    parseChunk(chunk);
  });

  readStream.on("end", () => {
    console.error("Sending result to the main thread");
    postMessage(map);
  });
}

/**
 * Range: -99.9 to 99.9
 * @param {Buffer} buffer
 * @param {number} length
 */
function parseFloatBufferIntoInt(buffer: Buffer, length: number) {
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
// /**
//  * @param {Buffer} b
//  * @param {number} length 1-5
//  *
//  * @returns {number}
//  */
// function parseFloatBufferIntoInt(b: Buffer, length: number): number {
//   if (b[0] === CHAR_MINUS) {
//     // b can be -1.1 or -11.1
//     switch (length) {
//       case 4:
//         return -(parseOneDigit(b[1]) * 10 + parseOneDigit(b[3]));
//       case 5:
//         return -(
//           parseOneDigit(b[1]) * 100 +
//           parseOneDigit(b[2]) * 10 +
//           parseOneDigit(b[4])
//         );
//     }
//   } else {
//     // b can be 1.1 or 11.1
//     switch (length) {
//       case 3: // b is 1.1
//         return parseOneDigit(b[0]) * 10 + parseOneDigit(b[2]);
//       case 4:
//         return (
//           parseOneDigit(b[0]) * 100 +
//           parseOneDigit(b[1]) * 10 +
//           parseOneDigit(b[3])
//         );
//     }
//   }

//   throw new Error("Failed to parse float buffer into int");
// }

/**
 * @param {number} char byte number of a digit char
 *
 * @returns {number}
 */
function parseDigit(char: number) {
  return char - 0x30;
}
