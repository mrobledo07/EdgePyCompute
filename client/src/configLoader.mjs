// configLoader.js
import fs from "fs/promises";

async function fileExists(filePath) {
  try {
    await fs.access(filePath);
    return true;
  } catch {
    return false;
  }
}

export async function loadConfig(filePath) {
  if (!(await fileExists(filePath))) {
    throw new Error(`Config file "${filePath}" does not exist.`);
  }

  const raw = await fs.readFile(filePath, "utf-8");
  const config = JSON.parse(raw);

  if (!config.type || !config.code || !config.args) {
    throw new Error(`Config must include 'type', 'code', and 'args'.`);
  }

  if (!Array.isArray(config.code) || config.code.length === 0) {
    throw new Error(`'code' must be a non-empty array.`);
  }

  if (!Array.isArray(config.args) || config.args.length === 0) {
    throw new Error(`'args' must be a non-empty array.`);
  }

  for (const file of config.code) {
    if (!(await fileExists(file))) {
      throw new Error(`Code file "${file}" does not exist.`);
    }
  }

  return config;
}

export async function loadCode(files) {
  return await Promise.all(files.map((file) => fs.readFile(file, "utf-8")));
}
