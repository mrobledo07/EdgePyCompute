// shared/state.js

// Clients connected via WebSocket, keyed by task ID
export const taskClients = new Map();

// MapReduce state (number of mappers/reducers left, intermediate results, etc.)
export const mapreduceTasks = new Map();
