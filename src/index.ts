import ClientDB from "./connectdb";

// ES Module exports
export default ClientDB;
export { ClientDB };

// CommonJS export for compatibility
// @ts-ignore
if (typeof module !== "undefined" && typeof module.exports !== "undefined") {
    module.exports = {
        default: ClientDB,
        ClientDB,
    };
}
