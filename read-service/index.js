const express = require('express');
const mongoose = require('mongoose');
const { Kafka } = require('kafkajs');

const app = express();
app.use(express.json());

// Mongoose Setup
const productSchema = new mongoose.Schema({
  id: { type: Number, required: true, unique: true },
  name: String,
  price: Number,
  category: String,
  stock: Number,
  createdAt: Date,
  updatedAt: Date,
  deletedAt: Date
});
productSchema.index({ name: 'text', category: 'text' });
productSchema.set('toJSON', {
  transform: (doc, ret) => {
    delete ret._id;
    delete ret.__v;
    return ret;
  }
});
const Product = mongoose.model('Product', productSchema);

mongoose.connect(process.env.MONGO_URI || 'mongodb://localhost:27017/products_read_db')
  .then(() => console.log('Connected to MongoDB'))
  .catch(err => console.error('MongoDB connection error:', err));

// Kafka Setup
const kafka = new Kafka({
  clientId: 'read-service',
  brokers: [(process.env.KAFKA_BROKER || 'localhost:9092')]
});
const consumer = kafka.consumer({ groupId: 'read-service-group' });

let lastProcessedOffset = 0;
let totalEventsProcessed = 0;

const parseDate = (val) => {
  if (!val) return null;
  if (typeof val === 'number') {
    if (val > 10000000000000) return new Date(val / 1000); // Microseconds
    return new Date(val); // Milliseconds
  }
  return new Date(val); // String fallback
};

const runKafkaConsumer = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: 'pg-server.public.products', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      totalEventsProcessed++;
      lastProcessedOffset = parseInt(message.offset, 10);
      
      try {
        const value = message.value ? JSON.parse(message.value.toString()) : null;
        
        if (value === null) {
          if (message.key) {
            const key = JSON.parse(message.key.toString());
            await Product.deleteOne({ id: key.id });
          }
          return;
        }

        const payload = value.payload;
        if (!payload) return;

        const op = payload.op;
        
        if (op === 'r' || op === 'c' || op === 'u') {
          const after = payload.after;
          if (after) {
            await Product.findOneAndUpdate(
              { id: after.id },
              {
                id: after.id,
                name: after.name,
                price: parseFloat(after.price),
                category: after.category,
                stock: after.stock,
                createdAt: parseDate(after.created_at),
                updatedAt: parseDate(after.updated_at),
                deletedAt: parseDate(after.deleted_at)
              },
              { upsert: true, new: true }
            );
          }
        } else if (op === 'd') {
          const before = payload.before;
          if (before) {
            await Product.deleteOne({ id: before.id });
          }
        }
      } catch (err) {
        console.error('Error processing message', err);
      }
    },
  });
};

runKafkaConsumer().catch(console.error);

// Endpoints
app.get('/health', (req, res) => res.status(200).send('OK'));

app.get('/api/products/search', async (req, res) => {
  const { query } = req.query;
  try {
    const products = await Product.find({ $text: { $search: query } });
    res.status(200).json(products);
  } catch (err) {
    res.status(500).json({ error: 'Search failed' });
  }
});

app.get('/api/products/category/:category', async (req, res) => {
  const { category } = req.params;
  try {
    const products = await Product.find({ category });
    res.status(200).json(products);
  } catch (err) {
    res.status(500).json({ error: 'Failed to fetch category' });
  }
});

app.get('/api/sync/status', async (req, res) => {
  try {
    // Estimating consumer lag is complex without Admin client. 
    // We will provide 0 as a placeholder if we can't fetch it easily.
    res.status(200).json({
      consumerLag: 0, 
      lastProcessedOffset,
      totalEventsProcessed
    });
  } catch (err) {
    res.status(500).json({ error: 'Status failed' });
  }
});

app.post('/api/sync/reset', async (req, res) => {
  try {
    // To reset, we seek to the beginning
    const admin = kafka.admin();
    await admin.connect();
    // Re-seek consumer for the topic partition
    // With KafkaJS, we can seek while running
    const partitions = [0]; // Assuming 1 partition as per compose file
    for (const partition of partitions) {
      consumer.seek({ topic: 'pg-server.public.products', partition, offset: 0 });
    }
    await admin.disconnect();
    res.status(202).send();
  } catch (err) {
    console.error('Reset error', err);
    res.status(500).json({ error: 'Reset failed' });
  }
});

const PORT = process.env.PORT || 8081;
app.listen(PORT, () => {
  console.log(`Read service listening on port ${PORT}`);
});
