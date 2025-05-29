const { MongoClient } = require('mongodb');

const uri = 'mongodb://localhost:27017';
const dbName = 'plp_bookstore';
const collectionName = 'books';

// -------------------- Basic Queries --------------------

// Find all books in a specific genre
async function findBooksByGenre(genre) {
  const client = new MongoClient(uri);
  try {
    await client.connect();
    const db = client.db(dbName);
    const collection = db.collection(collectionName);
    
    const books = await collection.find({ genre })
      .project({ title: 1, author: 1, price: 1, _id: 0 }) // Projection
      .toArray();

    console.log(`Books in genre "${genre}":`, books);
  } finally {
    await client.close();
  }
}

// Find books published after a certain year
async function findBooksAfterYear(year) {
  const client = new MongoClient(uri);
  try {
    await client.connect();
    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    const books = await collection.find({ published_year: { $gt: year } })
      .project({ title: 1, author: 1, price: 1, _id: 0 }) // Projection
      .toArray();

    console.log(`Books published after ${year}:`, books);
  } finally {
    await client.close();
  }
}

// Find books by a specific author
async function findBooksByAuthor(author) {
  const client = new MongoClient(uri);
  try {
    await client.connect();
    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    const books = await collection.find({ author })
      .project({ title: 1, author: 1, price: 1, _id: 0 }) // Projection
      .toArray();

    console.log(`Books by "${author}":`, books);
  } finally {
    await client.close();
  }
}

// -------------------- Update & Delete Operations --------------------

// Update the price of a specific book
async function updateBookPrice(title, newPrice) {
  const client = new MongoClient(uri);
  try {
    await client.connect();
    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    const result = await collection.updateOne({ title }, { $set: { price: newPrice } });
    console.log(`Updated price for "${title}". Matched: ${result.matchedCount}, Modified: ${result.modifiedCount}`);
  } finally {
    await client.close();
  }
}

// Delete a book by its title
async function deleteBookByTitle(title) {
  const client = new MongoClient(uri);
  try {
    await client.connect();
    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    const result = await collection.deleteOne({ title });
    console.log(`Deleted book "${title}". Deleted count: ${result.deletedCount}`);
  } finally {
    await client.close();
  }
}

// -------------------- Advanced Queries --------------------

// Find books that are both in stock and published after a certain year
async function findAvailableBooksAfterYear(year) {
  const client = new MongoClient(uri);
  try {
    await client.connect();
    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    const books = await collection.find({ in_stock: true, published_year: { $gt: year } })
      .project({ title: 1, author: 1, price: 1, _id: 0 }) // Projection
      .toArray();

    console.log(`Books in stock and published after ${year}:`, books);
  } finally {
    await client.close();
  }
}

// Sort books by price (ascending or descending)
async function findBooksSortedByPrice(order) {
  const client = new MongoClient(uri);
  try {
    await client.connect();
    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    const sortOrder = order === 'asc' ? 1 : -1;
    const books = await collection.find({})
      .project({ title: 1, author: 1, price: 1, _id: 0 }) // Projection
      .sort({ price: sortOrder }) // Sorting
      .toArray();

    console.log(`Books sorted by price (${order}):`, books);
  } finally {
    await client.close();
  }
}

// Implement pagination (5 books per page)
async function findBooksPaginated(page) {
  const client = new MongoClient(uri);
  try {
    await client.connect();
    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    const pageSize = 5;
    const skipCount = (page - 1) * pageSize;

    const books = await collection.find({})
      .project({ title: 1, author: 1, price: 1, _id: 0 }) // Projection
      .skip(skipCount) // Pagination
      .limit(pageSize)
      .toArray();

    console.log(`Books on page ${page}:`, books);
  } finally {
    await client.close();
  }
}

// -------------------- Aggregation Pipelines --------------------

// Calculate the average price of books by genre
async function averagePriceByGenre() {
  const client = new MongoClient(uri);
  try {
    await client.connect();
    const db = client.db(dbName);
    const collection = db.collection(collectionName);
    
    const pipeline = [
      { $group: { _id: "$genre", avgPrice: { $avg: "$price" } } },
      { $sort: { avgPrice: -1 } } // Sorting by average price (descending)
    ];

    const result = await collection.aggregate(pipeline).toArray();
    console.log("Average price by genre:", result);
  } finally {
    await client.close();
  }
}

// Find the author with the most books in the collection
async function authorWithMostBooks() {
  const client = new MongoClient(uri);
  try {
    await client.connect();
    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    const pipeline = [
      { $group: { _id: "$author", bookCount: { $sum: 1 } } },
      { $sort: { bookCount: -1 } }, // Sorting authors by book count (descending)
      { $limit: 1 } // Get the top author
    ];

    const result = await collection.aggregate(pipeline).toArray();
    console.log("Author with most books:", result);
  } finally {
    await client.close();
  }
}

// -------------------- Indexing --------------------

// Create an index on the "title" field
async function createTitleIndex() {
  const client = new MongoClient(uri);
  try {
    await client.connect();
    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    const result = await collection.createIndex({ title: 1 });
    console.log("Index created on 'title':", result);
  } finally {
    await client.close();
  }
}

// Create a compound index on "author" and "published_year"
async function createAuthorYearIndex() {
  const client = new MongoClient(uri);
  try {
    await client.connect();
    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    const result = await collection.createIndex({ author: 1, published_year: -1 });
    console.log("Compound index created on 'author' and 'published_year':", result);
  } finally {
    await client.close();
  }
}

// -------------------- Example Usage --------------------
// Uncomment functions to test them!

// Basic Queries
findBooksByGenre('Fiction');
findBooksAfterYear(1950);
findBooksByAuthor('J.R.R. Tolkien');

// Advanced Queries
averagePriceByGenre();
authorWithMostBooks();

// Indexing
createTitleIndex();
createAuthorYearIndex();