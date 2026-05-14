const express = require('express');
const { Pool } = require('pg');

const app = express();
app.use(express.json());

const pool = new Pool({
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  port: process.env.DB_PORT,
});

app.get('/health', async (req, res) => {
  try {
    await pool.query('SELECT 1');
    res.status(200).send('OK');
  } catch (err) {
    res.status(500).send('Database connection error');
  }
});

app.post('/api/products', async (req, res) => {
  const { name, price, category, stock } = req.body;
  try {
    const result = await pool.query(
      'INSERT INTO public.products (name, price, category, stock) VALUES ($1, $2, $3, $4) RETURNING id, name, price, category, stock, created_at AS "createdAt", updated_at AS "updatedAt", deleted_at AS "deletedAt"',
      [name, price, category, stock]
    );
    const product = result.rows[0];
    product.price = parseFloat(product.price);
    
    product.createdAt = product.createdAt.toISOString();
    product.updatedAt = product.updatedAt.toISOString();
    
    res.status(201).json(product);
  } catch (err) {
    console.error('Error creating product', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.put('/api/products/:id', async (req, res) => {
  const { id } = req.params;
  const { name, price, category, stock } = req.body;
  try {
    const result = await pool.query(
      'UPDATE public.products SET name = $1, price = $2, category = $3, stock = $4, updated_at = NOW() WHERE id = $5 AND deleted_at IS NULL RETURNING id, name, price, category, stock, created_at AS "createdAt", updated_at AS "updatedAt", deleted_at AS "deletedAt"',
      [name, price, category, stock, id]
    );
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Product not found' });
    }
    const product = result.rows[0];
    product.price = parseFloat(product.price);
    product.createdAt = product.createdAt.toISOString();
    product.updatedAt = product.updatedAt.toISOString();
    res.status(200).json(product);
  } catch (err) {
    console.error('Error updating product', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.delete('/api/products/:id', async (req, res) => {
  const { id } = req.params;
  try {
    const result = await pool.query(
      'UPDATE public.products SET deleted_at = NOW(), updated_at = NOW() WHERE id = $1 AND deleted_at IS NULL RETURNING id',
      [id]
    );
    res.status(204).send();
  } catch (err) {
    console.error('Error deleting product', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`Write service listening on port ${PORT}`);
});
