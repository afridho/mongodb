# afridho-mongodb

A simple and easy-to-use MongoDB client wrapper for Node.js applications. This package provides a straightforward interface for performing common database operations such as reading, inserting, updating, and deleting documents in a MongoDB collection.

## Table of Contents

-   [Installation](#installation)
-   [Setup](#setup)
-   [Usage](#usage)
-   [Methods](#methods)
-   [Requirements](#requirements)
-   [License](#license)
-   [Author](#author)
-   [Contributing](#contributing)
-   [Acknowledgments](#acknowledgments)

## Installation

To install the `afridho-mongodb` package, use npm:

```bash
npm install afridho-mongodb
```

## Setup

### Create a `.env` File

Before using the package, create a `.env` file in your project root with the following variables:

```env
MONGODB_URI=your_mongodb_connection_string
DB_NAME=your_database_name
```

## Usage

### Basic Example

Here’s a basic example of how to use the `afridho-mongodb` package:

```javascript
const ClientDB = require("afridho-mongodb");

async function main() {
    // Create a client for a specific collection
    const userCollection = new ClientDB("users");

    // Insert a new user
    await userCollection.insert({ name: "Alice", email: "alice@example.com" });

    // Read all users
    const users = await userCollection.readAll();
    console.log(users);

    // Update a user
    await userCollection.update({ name: "Alice" }, { age: 25 });

    // Delete a user
    await userCollection.delete({ name: "Alice" });

    // Close the connection
    await userCollection.close();
}

main().catch(console.error);
```

## Methods

The `ClientDB` class provides the following methods for interacting with your MongoDB collection:

-   **`connect()`**: Establishes a connection to the MongoDB database.

-   **`read(query)`**: Reads a single document from the collection based on the provided query.

    ```javascript
    const user = await userCollection.read({ name: "Alice" });
    console.log(user);
    ```

-   **`readAll()`**: Reads all documents from the collection.

    ```javascript
    const users = await userCollection.readAll();
    console.log(users);
    ```

-   **`insert(data)`**: Inserts a new document into the collection.

    ```javascript
    await userCollection.insert({ name: "Bob", email: "bob@example.com" });
    ```

-   **`insertMany(data)`**: Inserts multiple documents into the collection.

    ```javascript
    await userCollection.insertMany([
        { name: "Charlie", email: "charlie@example.com" },
        { name: "David", email: "david@example.com" },
    ]);
    ```

-   **`update(query, data)`**: Updates a document in the collection based on the provided query.

    ```javascript
    await userCollection.update({ name: "Bob" }, { age: 30 });
    ```

-   **`delete(query)`**: Deletes a single document from the collection based on the provided query.

    ```javascript
    await userCollection.delete({ name: "Bob" });
    ```

-   **`deleteMany(query)`**: Deletes multiple documents from the collection based on the provided query.

    ```javascript
    await userCollection.deleteMany({ age: { $gt: 30 } });
    ```

-   **`find(query)`**: Finds multiple documents in the collection based on the provided query.

    ```javascript
    const results = await userCollection.find({ age: { $lt: 30 } });
    console.log(results);
    ```

-   **`getStorageStats()`**: Gets the storage statistics for the collection.

    ```javascript
    const stats = await userCollection.getStorageStats();
    console.log(stats);
    ```

-   **`close()`**: Closes the MongoDB connection.
    ```javascript
    await userCollection.close();
    ```

## Requirements

-   Node.js 16+
-   MongoDB
-   dotenv

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Author

**afridho**  
[Github](https://github.com/afridho)

## Contributing

Contributions are welcome! Please feel free to submit a pull request or open an issue. To contribute:

1. Fork the repository.
2. Create a new branch (`git checkout -b feature/YourFeature`).
3. Make your changes.
4. Commit your changes (`git commit -m 'Add some feature'`).
5. Push to the branch (`git push origin feature/YourFeature`).
6. Open a pull request.

## Acknowledgments

-   [MongoDB](https://www.mongodb.com/) for the database.
-   [dotenv](https://www.npmjs.com/package/dotenv) for environment variable management.
