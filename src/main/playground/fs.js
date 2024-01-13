const fs = require("node:fs");
const fsp = require("node:fs/promises");
const os = require("node:os");

function readInPosition(filename, start, end) {
  const readStream = fs.createReadStream(filename, {
    start,
    end,
  });

  readStream.on("data", (/** @type {Buffer} */ chunk) => {
    console.log(chunk.toString());
  });

  readStream.on("end", () => {
    console.log("Bytes read = ", readStream.bytesRead);
    console.log("\n\n\n");
  });
}

/**
 *
 * @param {fs.promises.FileHandle} file
 * @param {Number} startFillingBufferFrom - The location in the buffer at which to start filling.
 * @param {Number} numberOfBytes
 * @param {Number | null} readFromPosition - from which position to start reading the file
 * @param {Number} chunkSize - number of bytes per chunk
 */
async function readIntoBuffer(file, chunkSize) {
  const MAX_LINE_LENGTH = 106; // as per challenge input constraint
  const START_FILLING_BUFFER_FROM = 0;
  const NUM_BYTES_TO_READ = MAX_LINE_LENGTH;
  const size = (await file.stat()).size;
  const buffer = Buffer.alloc(MAX_LINE_LENGTH);

  let offset = 0; // from which position to start reading the file (updated in each iter)
  const chunks = [];
  while (true) {
    offset += chunkSize;
    if (offset >= size) {
      chunks.push(size);
      break;
    }
    await file.read(
      buffer,
      START_FILLING_BUFFER_FROM,
      NUM_BYTES_TO_READ,
      offset
    );
    const nlPos = buffer.indexOf(10);
    buffer.fill(0);
    if (nlPos === -1) {
      console.log("new line didnt exist here....");
      chunks.push(size);
      break;
    } else {
      offset += nlPos + 1;
      chunks.push(offset);
    }
  }

  await file.close();
  return chunks;
}

async function go() {
  const filename = process.argv[2];
  if (!filename) throw new Error("Filename not provided");
  const file = await fsp.open(filename);
  const size = (await file.stat()).size;
  const numThreads = os.cpus().length;
  console.log("File size: (kb)", size);
  const chunkSize = Math.floor(size / numThreads);
  console.log("Chunks of = ", chunkSize);

  const chunkOffsets = await readIntoBuffer(file, chunkSize);
  let pos = 0;
  for (const chunk of chunkOffsets) {
    readInPosition(filename, pos === 0 ? 0 : chunkOffsets[pos - 1], chunk - 1);
    pos += 1;
  }
}

go();
