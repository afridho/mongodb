import { ClientDB, ObjectId } from "./connectdb";

// ES Module exports
export default ClientDB;
export { ClientDB, ObjectId };

// CommonJS export for compatibility
// @ts-ignore
if (typeof module !== "undefined" && typeof module.exports !== "undefined") {
    module.exports = ClientDB;
    module.exports.ClientDB = ClientDB;
    module.exports.ObjectId = ObjectId;
}
