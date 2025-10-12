const express = require('express')
const app = express()
const port = 7700
const cors = require('cors')
const path = require('path') // <-- existing
const fs = require('fs') // <-- added
const axios = require('axios'); // <-- add this
// --- New: JSON body parsing middleware ---

app.use(cors())
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ limit: '50mb', extended: true }));
app.get('/', (req, res) => {
  res.send('Hello World!')
})

app.get('/airports', (req, res) => {
  console.log('GET /airports called');
  res.json(airportsData);
})
// --- New: mongoose for DB CRUD ---
const mongoose = require('mongoose');
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb+srv://ngophuc2911_db_user:phuc29112003@cluster0.xrujamk.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0';

// Use provided connection string (or override with env var)
const sharp = require('sharp');
const streamifier = require('streamifier');
const cloudinary = require('cloudinary').v2;

// cloudinary config (keep your hardcoded or env)
cloudinary.config({
  cloud_name: process.env.CLOUDINARY_NAME || 'dxm8pqql5',
  api_key: process.env.CLOUDINARY_APIKEY || '973126759771237',
  api_secret: process.env.CLOUDINARY_APISECRET || '_sIE_D41tWHju2nEbmOHC4OrVcg',
});
const multer = require('multer');
const storage = multer.memoryStorage();
const upload = multer({
  storage,
  limits: { fileSize: 10 * 1024 * 1024 }, // 10MB per file
  fileFilter: (req, file, cb) => {
    const allowed = ['image/jpeg', 'image/png', 'image/webp', 'image/gif'];
    cb(null, allowed.includes(file.mimetype));
  },
});

// helper: upload buffer to cloudinary via upload_stream
function uploadBufferToCloudinary(buffer, folder = 'articles') {
  return new Promise((resolve, reject) => {
    const uploadStream = cloudinary.uploader.upload_stream({ folder }, (error, result) => {
      if (error) return reject(error);
      resolve(result);
    });
    streamifier.createReadStream(buffer).pipe(uploadStream);
  });
}
async function uploadEmbeddedImagesInHtml(html, opts = {}) {
  if (!html || typeof html !== 'string') return html;
  const maxBytes = opts.maxBytes || 10 * 1024 * 1024; // server side limit per image
  const maxWidth = opts.maxWidth || 1600;
  const quality = typeof opts.quality === 'number' ? opts.quality : 80;

  const regex = /<img[^>]+src=(["'])(data:[^"'>]+)\1[^>]*>/g;
  const found = new Map();
  let match;
  const uploads = [];

  while ((match = regex.exec(html)) !== null) {
    const dataUri = match[2];
    if (!dataUri || !dataUri.startsWith('data:')) continue;
    if (found.has(dataUri)) continue;
    found.set(dataUri, true);

    uploads.push((async () => {
      // split metadata
      const comma = dataUri.indexOf(',');
      const meta = dataUri.slice(5, comma); // e.g. image/jpeg;base64
      const b64 = dataUri.slice(comma + 1);
      const buffer = Buffer.from(b64, 'base64');
      if (buffer.length > maxBytes) {
        // still attempt to compress with sharp (reduce size)
      }
      // use sharp to resize and compress
      let img = sharp(buffer).rotate();
      const metaInfo = await img.metadata();
      if (metaInfo.width && metaInfo.width > maxWidth) {
        img = img.resize({ width: maxWidth, withoutEnlargement: true });
      }
      // choose output format
      if (metaInfo.format === 'png') {
        img = img.png({ quality });
      } else {
        img = img.jpeg({ quality });
      }
      const outBuffer = await img.toBuffer();
      // upload to cloudinary
      const result = await uploadBufferToCloudinary(outBuffer, opts.folder || 'articles');
      return { dataUri, url: result.secure_url || result.url };
    })());
  }

  if (uploads.length === 0) return html;
  const results = await Promise.all(uploads);
  let newHtml = html;
  for (const r of results) {
    const esc = r.dataUri.replace(/[-/\\^$*+?.()|[\]{}]/g, '\\$&');
    newHtml = newHtml.replace(new RegExp(esc, 'g'), r.url);
  }
  return newHtml;
}
// --- New: initialize DB then start server to avoid mongoose buffering timeouts ---
(async function init() {
  try {
    await mongoose.connect(MONGODB_URI, {
      useNewUrlParser: true,
      useUnifiedTopology: true,
      serverSelectionTimeoutMS: 10000 // fail fast if cannot reach DB
    });
    console.log('Connected to MongoDB');

    // start server only after successful DB connection
    app.listen(port, () => {
      console.log(`Server bus ,promotion,order,support listening on port ${port}`);
    });
  } catch (err) {
    console.error('MongoDB connection error:', err);
    // Exit so the process doesn't accept requests while DB unavailable
    process.exit(1);
  }
})();




const DateSeatSchema = new mongoose.Schema({
  seatId: { type: String, required: true },
  label: String,
  type: String,
  pos: { r: Number, c: Number },
  status: { type: String, enum: ['available', 'booked', 'blocked'], default: 'available' },
  reservationId: { type: String, default: null }
}, { _id: false });

const DateBookingSchema = new mongoose.Schema({
  dateIso: { type: String, required: true }, // YYYY-MM-DD
  seatsTotal: { type: Number, default: 0 },
  seatsAvailable: { type: Number, default: 0 },
  seatReserved: { type: Number, default: 0 },
  // log of seat bookings for audit / release by reservationId or orderNumber
  logSeatBooked: {
    type: [new mongoose.Schema({
      seatId: String,
      reservationId: String,
      orderNumber: String,
      customerId: String,
      status: { type: String, enum: ['confirm', 'cancel'], default: 'confirm' }, // NEW: track confirm/cancel
      ts: { type: Date, default: Date.now },
      cancelledAt: { type: Date, default: null }
    }, { _id: false })],
    default: []
  },
  seatmapFill: { type: [DateSeatSchema], default: [] }
}, { _id: false });

const BusSlotSchema = new mongoose.Schema({
  busId: { type: mongoose.Schema.Types.ObjectId, required: true, index: true, unique: true },
  dateBookings: { type: [DateBookingSchema], default: [] },
  createdAt: { type: Date, default: Date.now }
}, { collection: 'bus_slots' });

const BusSlot = mongoose.model('BusSlot', BusSlotSchema);
function normalizeDateIso(v) {
  return toDateIso(v);
}

// helper: build seatmapFill base from bus.seatMap (reservationId null preserved only if present)
function buildSeatmapFillFromBus(bus) {
  const map = Array.isArray(bus.seatMap) ? bus.seatMap : [];
  return map.map(s => ({
    seatId: s.seatId || s.label || '',
    label: s.label || s.seatId || '',
    type: s.type || 'seat',
    pos: s.pos || {},
    status: (s.status === 'booked' || s.status === 'blocked') ? s.status : 'available',
    reservationId: s.reservationId || null
  }));
}

// ensure BusSlot exists for a bus (create per-departure date entries)
async function ensureBusSlotsForBus(bus) {
  try {
    if (!bus) {
      console.warn('ensureBusSlotsForBus skipped: no bus passed');
      return;
    }

    // normalize busId and build safe query id
    const busIdVal = (bus && bus._id) ? bus._id : bus;
    let queryBusId = busIdVal;
    if (mongoose.isValidObjectId(busIdVal)) {
      // use constructor with `new` to avoid "cannot be invoked without 'new'"
      queryBusId = new mongoose.Types.ObjectId(String(busIdVal));
    }

    console.log('ensureBusSlotsForBus start', { busId: String(busIdVal), departureDatesCount: Array.isArray(bus.departureDates) ? bus.departureDates.length : (bus.departureAt ? 1 : 0) });

    const existing = await BusSlot.findOne({ busId: queryBusId }).lean();
    if (existing) {
      console.log('ensureBusSlotsForBus: BusSlot already exists for', String(busIdVal));
      return;
    }

    const dates = Array.isArray(bus.departureDates) && bus.departureDates.length
      ? bus.departureDates
      : (bus.departureAt ? [bus.departureAt] : []);

    const masterSeatmap = buildSeatmapFillFromBus(bus);
    const seatsTotal = masterSeatmap.length || Number(bus.seatsTotal || 0);

    const dateBookings = (dates || []).map(d => {
      const dateIso = normalizeDateIso(d);
      if (!dateIso) return null;

      const initialLog = (masterSeatmap || []).filter(s => s.status === 'booked').map(s => ({
        seatId: s.seatId,
        reservationId: s.reservationId || null,
        orderNumber: null,
        customerId: null,
        ts: new Date()
      }));

      const seatmapFill = masterSeatmap.map(m => {
        const isBooked = initialLog.some(l => l.seatId === m.seatId);
        return {
          ...m,
          status: isBooked ? 'booked' : 'available',
          reservationId: isBooked ? (m.reservationId || null) : null
        };
      });

      const seatReserved = initialLog.length;
      const seatsAvailable = Math.max(0, seatsTotal - seatReserved);

      return {
        dateIso,
        seatsTotal,
        seatsAvailable,
        seatReserved,
        logSeatBooked: initialLog,
        seatmapFill
      };
    }).filter(Boolean);

    if (dateBookings.length === 0) {
      console.log('ensureBusSlotsForBus: no dateBookings produced for', String(busIdVal));
      return;
    }

    const slotDoc = new BusSlot({ busId: queryBusId, dateBookings });
    await slotDoc.save();
    // debug logs to confirm persistence and DB being used
    try {
      console.log('BusSlot created for busId', String(busIdVal), 'datesCount', dateBookings.length, 'slotId', String(slotDoc._id));
      console.log('Mongoose DB name:', mongoose.connection && mongoose.connection.name);
      // double-check by reading back
      const check = await BusSlot.findOne({ busId: queryBusId }).lean();
      if (!check) console.warn('Verification read returned null for saved BusSlot');
      else console.log('Verification read succeeded, docId:', String(check._id));
    } catch (e) {
      console.warn('Post-save verification failed', e && e.message ? e.message : e);
    }
    console.log('BusSlot created for busId', String(busIdVal), 'datesCount', dateBookings.length);
  } catch (err) {
    console.error('ensureBusSlotsForBus error', err && err.message ? err.message : err);
  }
}
app.get('/api/bus-slots/:busId', async (req, res) => {
  try {
    const bid = req.params.busId;
    let queryId = bid;
    if (mongoose.Types.ObjectId.isValid(bid)) queryId = new mongoose.Types.ObjectId(String(bid));
    const doc = await BusSlot.findOne({ busId: queryId }).lean();
    if (!doc) return res.status(404).json({ error: 'BusSlot not found' });
    return res.json(doc);
  } catch (err) {
    console.error('GET /api/bus-slots/:busId error', err);
    return res.status(500).json({ error: 'failed', details: err.message });
  }
});

// also adjust sync to use same safe conversion and logging
async function syncBusSlotsForBus(bus) {
  try {
    if (!bus) {
      console.warn('syncBusSlotsForBus skipped: no bus passed');
      return;
    }
    const busIdVal = (bus && bus._id) ? bus._id : bus;
    let queryBusId = busIdVal;
    if (mongoose.isValidObjectId(busIdVal)) {
      queryBusId = new mongoose.Types.ObjectId(String(busIdVal));
    }

    const slotDoc = await BusSlot.findOne({ busId: queryBusId });
    if (!slotDoc) {
      console.log('syncBusSlotsForBus: no BusSlot found for', String(busIdVal));
      return;
    }

    const masterSeatmap = buildSeatmapFillFromBus(bus);
    const seatsTotal = masterSeatmap.length || Number(bus.seatsTotal || 0);

    for (const dbEntry of slotDoc.dateBookings) {
      const oldById = new Map((dbEntry.seatmapFill || []).map(s => [s.seatId, s]));
      const newFill = masterSeatmap.map(m => {
        const old = oldById.get(m.seatId);
        const status = (old && (old.status === 'booked' || old.status === 'blocked')) ? old.status
          : (m.status === 'booked' || m.status === 'blocked' ? m.status : 'available');
        const reservationId = (old && old.reservationId) ? old.reservationId : (m.reservationId || null);
        return {
          seatId: m.seatId,
          label: m.label || m.seatId,
          type: m.type || 'seat',
          pos: m.pos || {},
          status,
          reservationId
        };
      });

      dbEntry.logSeatBooked = (dbEntry.logSeatBooked || []).filter(l => newFill.some(s => s.seatId === l.seatId));

      const reservedCountFromMap = newFill.filter(s => s.status === 'booked' || s.reservationId).length;
      const reservedCountFromLog = (dbEntry.logSeatBooked || []).length;

      dbEntry.seatsTotal = seatsTotal;
      dbEntry.seatmapFill = newFill;
      dbEntry.seatReserved = Math.max(reservedCountFromMap, reservedCountFromLog);
      dbEntry.seatsAvailable = Math.max(0, dbEntry.seatsTotal - dbEntry.seatReserved);
    }

    await slotDoc.save();
    console.log(`Synced BusSlot for bus ${String(busIdVal)}`);
  } catch (err) {
    console.error('syncBusSlotsForBus error', err && err.message ? err.message : err);
  }
}

// Replace previous BusSchema definition with improved schema + validations
const SeatSchema = new mongoose.Schema({
  seatId: { type: String, required: true },
  label: String,
  type: String,
  pos: {
    r: Number,
    c: Number
  },
  status: { type: String, enum: ['available', 'booked', 'blocked'], default: 'available' }
}, { _id: false });

const BusSchema = new mongoose.Schema({
  busCode: { type: String, required: true, index: true }, // consider unique if needed
  operatorId: { type: String, default: null }, // accept operatorId from client
  operator: {
    id: String, name: String, logo: String, code: String
  },
  routeFrom: { code: String, name: String, city: String },
  routeTo: { code: String, name: String, city: String },
  departureAt: { type: Date, required: false },
  // new: multiple departure dates
  departureDates: { type: [Date], default: [] },
  // new: multiple arrival dates (paired by index with departureDates)
  arrivalDates: { type: [Date], default: [] },
  arrivalAt: { type: Date, required: false },
  duration: String,
  busType: [String],
  adultPrice: { type: Number, min: 0, default: 0 },
  childPrice: { type: Number, min: 0, default: 0 },
  seatsTotal: { type: Number, min: 0, default: 0 },
  seatsAvailable: { type: Number, min: 0, default: 0 },
  // seatMap stored as array of seat objects with status
  seatMap: { type: [SeatSchema], default: [] },
  status: { type: String, enum: ["scheduled", "cancelled", "delayed", "completed"], default: "scheduled" },
  // change amenities from String -> array of strings
  amenities: { type: [String], default: [] },
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now },
}, { collection: 'buses' });

// Validate logical consistency before saving
BusSchema.pre('validate', function (next) {
  try {
    // Backfill adultPrice from legacy price if present (handle old docs or incoming payloads)
    if ((typeof this.adultPrice !== 'number' || isNaN(this.adultPrice)) && typeof this.price !== 'undefined') {
      // if legacy field exists (from older docs), use it
      const p = Number(this.price);
      if (!Number.isNaN(p)) this.adultPrice = p;
    }
    // ensure adultPrice/childPrice are numbers >= 0
    if (typeof this.adultPrice !== 'number' || isNaN(this.adultPrice) || this.adultPrice < 0) this.adultPrice = Math.max(0, Number(this.adultPrice) || 0);
    if (typeof this.childPrice !== 'number' || isNaN(this.childPrice) || this.childPrice < 0) this.childPrice = Math.max(0, Number(this.childPrice) || 0);

    // Normalize departureDates: ensure array of valid Dates
    if (Array.isArray(this.departureDates) && this.departureDates.length > 0) {
      const parsed = this.departureDates.map(d => new Date(d));
      // remove invalid ones
      const valid = parsed.filter(d => !isNaN(d.getTime()));
      this.departureDates = valid;
      // set departureAt to earliest date for backward compat
      if (!this.departureAt && valid.length) {
        const earliest = valid.reduce((a, b) => a < b ? a : b);
        this.departureAt = earliest;
      } else if (valid.length) {
        // ensure departureAt is at least set to earliest to be consistent
        const earliest = valid.reduce((a, b) => a < b ? a : b);
        if (!this.departureAt || new Date(this.departureAt) > earliest) {
          this.departureAt = earliest;
        }
      }
    }

    // Normalize arrivalDates: ensure array of valid Dates and set arrivalAt to earliest (or first) if not set
    if (Array.isArray(this.arrivalDates) && this.arrivalDates.length > 0) {
      const parsedArr = this.arrivalDates.map(d => new Date(d)).filter(d => !isNaN(d.getTime()));
      this.arrivalDates = parsedArr;
      if (!this.arrivalAt && parsedArr.length) {
        this.arrivalAt = parsedArr[0];
      }
    }

    // Ensure arrival is after departure when both provided:
    if (this.arrivalAt) {
      // if there are departureDates, ensure arrival > earliest departure
      if (Array.isArray(this.departureDates) && this.departureDates.length > 0) {
        const earliest = this.departureDates.reduce((a, b) => (a < b ? a : b));
        if (new Date(this.arrivalAt) <= new Date(earliest)) {
          return next(new Error('arrivalAt must be after earliest departure date'));
        }
      } else if (this.departureAt) {
        if (new Date(this.arrivalAt) <= new Date(this.departureAt)) {
          return next(new Error('arrivalAt must be after departureAt'));
        }
      }
    }

    // If seatMap provided, derive seatsTotal and seatsAvailable from it
    if (Array.isArray(this.seatMap) && this.seatMap.length > 0) {
      const totalFromMap = this.seatMap.length;
      const booked = this.seatMap.filter(s => s.status === 'booked').length;
      this.seatsTotal = totalFromMap;
      // if seatsAvailable explicitly provided, clamp it; otherwise compute from map
      if (typeof this.seatsAvailable === 'number') {
        this.seatsAvailable = Math.min(Math.max(0, this.seatsAvailable), this.seatsTotal);
      } else {
        this.seatsAvailable = Math.max(0, this.seatsTotal - booked);
      }
    } else {
      // No seatMap: ensure seatsTotal/seatsAvailable are sensible numbers
      if (typeof this.seatsTotal !== 'number' || this.seatsTotal < 0) this.seatsTotal = 0;
      if (typeof this.seatsAvailable !== 'number' || this.seatsAvailable < 0) this.seatsAvailable = Math.max(0, this.seatsTotal);
      if (this.seatsAvailable > this.seatsTotal) this.seatsAvailable = this.seatsTotal;
    }

    // Normalize amenities stored as comma-separated string into array
    if (typeof this.amenities === 'string') {
      this.amenities = this.amenities.split(',').map(s => String(s).trim()).filter(Boolean);
    }
    if (!Array.isArray(this.amenities)) {
      this.amenities = [];
    }

    return next();
  } catch (err) {
    return next(err);
  }
});

// Keep updatedAt on save
BusSchema.pre('save', function (next) {
  this.updatedAt = new Date();
  next();
});

const Bus = mongoose.model('Bus', BusSchema);




// thêm đường dẫn tới 2 file JSON (ở thư mục cha)
const busRawFile = path.join(__dirname, '..', 'cac_ben_xe_bus_chua_sat_nhap.json')
const busNormalizedFile = path.join(__dirname, '..', 'cac_ben_xe_bus_sau_sat_nhap.json')

// --- New: đường dẫn tới file nhà xe ---
const nhaxeFile = path.join(__dirname, '..', 'nhaxekhach.json')

// --- New: đường dẫn tới file loại xe (loaixe.json) ---
const loaixeFile = path.join(__dirname, '..', 'dsloaixevexere.json')

// --- New API: CRUD for buses ---
// List with pagination & filters: GET /api/buses
app.get('/api/buses', async (req, res) => {
  try {
    const page = Math.max(1, parseInt(req.query.page) || 1);
    const pageSize = Math.max(1, parseInt(req.query.pageSize) || 10);
    const search = req.query.search ? String(req.query.search).trim() : '';
    const operator = req.query.operator ? String(req.query.operator) : 'all';
    const status = req.query.status ? String(req.query.status) : 'all';
    const route = req.query.route ? String(req.query.route) : 'all';

    const filter = {};
    if (search) {
      const re = new RegExp(search, 'i');
      filter.$or = [
        { busCode: re },
        { 'operator.name': re },
        { 'routeFrom.city': re },
        { 'routeTo.city': re },
      ];
    }
    if (operator && operator !== 'all') {
      filter['operator.id'] = operator;
    }
    if (status && status !== 'all') {
      filter.status = status;
    }
    if (route && route !== 'all') {
      // route param format example: "SGN_MB-DAD_BX" or partial
      filter.$or = filter.$or || [];
      const re = new RegExp(route, 'i');
      filter.$or.push({ 'routeFrom.code': re }, { 'routeTo.code': re });
    }

    const total = await Bus.countDocuments(filter);
    let buses = await Bus.find(filter)
      .sort({ departureAt: 1 })
      .skip((page - 1) * pageSize)
      .limit(pageSize)
      .lean();

    // normalize amenities on each bus for consistent API shape
    buses = buses.map(b => {
      if (typeof b.amenities === 'string') b.amenities = b.amenities.split(',').map((s) => s.trim()).filter(Boolean);
      if (!Array.isArray(b.amenities)) b.amenities = [];
      return b;
    });

    return res.json({
      data: buses,
      pagination: { total, current: page, pageSize }
    });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: 'Failed to fetch buses' });
  }
});

// Get single bus
app.get('/api/buses/:id', async (req, res) => {
  try {
    const bus = await Bus.findById(req.params.id).lean();
    if (!bus) return res.status(404).json({ error: 'Bus not found' });

    // normalize amenities before sending
    if (typeof bus.amenities === 'string') {
      bus.amenities = bus.amenities.split(',').map((s) => s.trim()).filter(Boolean);
    } else if (!Array.isArray(bus.amenities)) {
      bus.amenities = [];
    }
    // backfill adultPrice from legacy price if needed and remove legacy key
    if ((typeof bus.adultPrice !== 'number' || isNaN(bus.adultPrice)) && typeof bus.price === 'number') {
      bus.adultPrice = Number(bus.price) || 0;
    }
    if (typeof bus.childPrice !== 'number') bus.childPrice = bus.childPrice || 0;
    if (typeof bus.price !== 'undefined') delete bus.price;

    return res.json(bus);
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: 'Failed to fetch bus' });
  }
});

// Create bus
app.post('/api/buses', async (req, res) => {
  try {
    const payload = req.body || {};

    // Normalize departureDates -> array of valid Dates and set departureAt (earliest) for backward compat
    if (Array.isArray(payload.departureDates) && payload.departureDates.length) {
      payload.departureDates = payload.departureDates
        .map(d => new Date(d))
        .filter(d => !isNaN(d.getTime()))
        .sort((a, b) => a.getTime() - b.getTime());
      if (payload.departureDates.length) {
        payload.departureAt = payload.departureAt ? new Date(payload.departureAt) : payload.departureDates[0];
      }
    } else if (payload.departureAt) {
      const dt = new Date(payload.departureAt);
      if (!isNaN(dt.getTime())) {
        payload.departureAt = dt;
        payload.departureDates = [dt];
      } else {
        payload.departureDates = [];
      }
    } else {
      payload.departureDates = [];
    }

    // Normalize arrivalDates -> array of valid Dates and set arrivalAt (first) for backward compat
    if (Array.isArray(payload.arrivalDates) && payload.arrivalDates.length) {
      payload.arrivalDates = payload.arrivalDates
        .map(d => new Date(d))
        .filter(d => !isNaN(d.getTime()));
      if (payload.arrivalDates.length) {
        payload.arrivalAt = payload.arrivalAt ? new Date(payload.arrivalAt) : payload.arrivalDates[0];
      }
    } else if (payload.arrivalAt) {
      const at = new Date(payload.arrivalAt);
      if (!isNaN(at.getTime())) {
        payload.arrivalAt = at;
        payload.arrivalDates = [at];
      } else {
        payload.arrivalDates = [];
      }
    } else {
      payload.arrivalDates = [];
    }

    // Compute duration from the first departure/arrival pair (if available)
    if (payload.departureDates.length && payload.arrivalDates.length) {
      const dep = new Date(payload.departureDates[0]);
      const arr = new Date(payload.arrivalDates[0]);
      if (!isNaN(dep.getTime()) && !isNaN(arr.getTime()) && arr.getTime() > dep.getTime()) {
        const diffMs = arr.getTime() - dep.getTime();
        const hours = Math.floor(diffMs / (1000 * 60 * 60));
        const minutes = Math.floor((diffMs % (1000 * 60 * 60)) / (1000 * 60));
        payload.duration = `${hours}h ${minutes}m`;
      }
    } else if (payload.departureAt && payload.arrivalAt) {
      const dep = new Date(payload.departureAt);
      const arr = new Date(payload.arrivalAt);
      if (!isNaN(dep.getTime()) && !isNaN(arr.getTime()) && arr.getTime() > dep.getTime()) {
        const diffMs = arr.getTime() - dep.getTime();
        const hours = Math.floor(diffMs / (1000 * 60 * 60));
        const minutes = Math.floor((diffMs % (1000 * 60 * 60)) / (1000 * 60));
        payload.duration = `${hours}h ${minutes}m`;
      }
    }

    // seatsTotal / seatsAvailable sanitization
    payload.seatsTotal = Number.isFinite(Number(payload.seatsTotal)) ? Number(payload.seatsTotal) : 0;
    payload.seatsAvailable = typeof payload.seatsAvailable === 'number'
      ? payload.seatsAvailable
      : (Number.isFinite(Number(payload.seatsAvailable)) ? Number(payload.seatsAvailable) : payload.seatsTotal);

    if (payload.seatsAvailable < 0) payload.seatsAvailable = 0;
    if (payload.seatsAvailable > payload.seatsTotal) payload.seatsAvailable = payload.seatsTotal;

    // Normalize seatMap items (ensure seatId and allowed status values), derive seatsTotal/seatsAvailable when seatMap provided
    if (Array.isArray(payload.seatMap) && payload.seatMap.length) {
      payload.seatMap = payload.seatMap.map(s => ({
        seatId: s.seatId || s.id || '',
        label: s.label || s.seatId || '',
        type: s.type || 'seat',
        pos: (s.pos && typeof s.pos === 'object') ? { r: s.pos.r || null, c: s.pos.c || null } : undefined,
        status: (s.status === 'booked' || s.status === 'blocked') ? s.status : 'available'
      }));

      // derive totals from seatMap unless user explicitly provided seatsTotal/seatsAvailable (we prioritize seatMap)
      const totalFromMap = payload.seatMap.length;
      const bookedFromMap = payload.seatMap.filter(s => s.status === 'booked').length;
      payload.seatsTotal = totalFromMap;
      if (typeof req.body.seatsAvailable === 'number') {
        payload.seatsAvailable = Math.min(Math.max(0, payload.seatsAvailable), payload.seatsTotal);
      } else {
        payload.seatsAvailable = Math.max(0, payload.seatsTotal - bookedFromMap);
      }
    }

    // Normalize amenities: accept string or array, store array
    if (typeof payload.amenities === 'string') {
      payload.amenities = payload.amenities.split(',').map((s) => String(s).trim()).filter(Boolean);
    } else if (!Array.isArray(payload.amenities)) {
      payload.amenities = [];
    } else {
      payload.amenities = payload.amenities.map((s) => String(s).trim()).filter(Boolean);
    }

    // Create and save
    const bus = new Bus(payload);
    await bus.save();

    // ensure BusSlot exists for this bus (create slot docs for departureDates)
    try {
      await ensureBusSlotsForBus(bus);
    } catch (e) {
      console.warn('ensureBusSlotsForBus failed after create', e && e.message ? e.message : e);
    }

    return res.status(201).json(bus);
  } catch (err) {
    console.error('POST /api/buses error:', err);
    return res.status(400).json({ error: 'Failed to create bus', details: err.message });
  }
});

// Update bus
app.put('/api/buses/:id', async (req, res) => {
  try {
    const payload = req.body;
    // map legacy price -> adultPrice for updates; then remove price
    if (typeof payload.adultPrice === 'undefined' && typeof payload.price !== 'undefined') {
      payload.adultPrice = Number(payload.price) || 0;
    }
    if (typeof payload.adultPrice !== 'undefined') payload.adultPrice = Number(payload.adultPrice);
    if (typeof payload.childPrice !== 'undefined') payload.childPrice = Number(payload.childPrice);
    delete payload.price;

    // Normalize departureDates/arrivalAt similar to create
    if (Array.isArray(payload.departureDates)) {
      payload.departureDates = payload.departureDates.map(d => new Date(d)).filter(d => !isNaN(d.getTime()));
      if (payload.departureDates.length) {
        payload.departureAt = payload.departureDates[0];
      }
    } else if (payload.departureAt) {
      payload.departureAt = new Date(payload.departureAt);
      if (!Array.isArray(payload.departureDates)) payload.departureDates = [payload.departureAt];
    }

    if (payload.arrivalAt) payload.arrivalAt = new Date(payload.arrivalAt);

    // compute duration when both departureAt and arrivalAt available
    if (payload.departureAt && payload.arrivalAt) {
      const diffMs = new Date(payload.arrivalAt).getTime() - new Date(payload.departureAt).getTime();
      if (diffMs > 0) {
        const hours = Math.floor(diffMs / (1000 * 60 * 60));
        const minutes = Math.floor((diffMs % (1000 * 60 * 60)) / (1000 * 60));
        payload.duration = `${hours}h ${minutes}m`;
      }
    }

    // Normalize amenities for update payload
    if (payload && typeof payload.amenities === 'string') {
      payload.amenities = payload.amenities.split(',').map((s) => String(s).trim()).filter(Boolean);
    } else if (payload && Array.isArray(payload.amenities)) {
      payload.amenities = payload.amenities.map((s) => String(s).trim()).filter(Boolean);
    }

    payload.updatedAt = new Date();
    const bus = await Bus.findByIdAndUpdate(req.params.id, payload, { new: true, runValidators: true }).lean();
    if (!bus) return res.status(404).json({ error: 'Bus not found' });

    // ensure/sync BusSlot after bus update (create if missing and sync layout/reservations)
    try {
      // ensureBusSlotsForBus accepts both mongoose doc and plain object (uses _id)
      await ensureBusSlotsForBus(bus);
      await syncBusSlotsForBus(bus);
    } catch (e) {
      console.warn('syncBusSlotsForBus failed after update', e && e.message ? e.message : e);
    }


    // ensure adultPrice/childPrice exist in response
    if (typeof bus.adultPrice !== 'number') bus.adultPrice = 0;
    if (typeof bus.childPrice !== 'number') bus.childPrice = bus.childPrice || 0;

    // ensure response amenities is always an array (handle old docs)
    if (bus && typeof bus.amenities === 'string') {
      bus.amenities = bus.amenities.split(',').map(s => s.trim()).filter(Boolean);
    }

    return res.json(bus);
  } catch (err) {
    console.error(err);
    return res.status(400).json({ error: 'Failed to update bus', details: err.message });
  }
});

// Delete bus
app.delete('/api/buses/:id', async (req, res) => {
  try {
    const id = req.params.id;
    let removed = null;

    // if id looks like a Mongo ObjectId try deleting by _id first
    if (mongoose.Types.ObjectId.isValid(id)) {
      removed = await Bus.findByIdAndDelete(id).lean();
    }

    // if not found (or id not an ObjectId), try deleting by busCode or custom id field
    if (!removed) {
      removed = await Bus.findOneAndDelete({ $or: [{ busCode: id }, { id: id }] }).lean();
    }

    if (!removed) return res.status(404).json({ error: 'Bus not found' });
    // remove associated BusSlot doc (best-effort)
    try {
      if (removed._id) {
        await BusSlot.deleteOne({ busId: removed._id }).catch(e => console.warn('delete BusSlot failed', e && e.message ? e.message : e));
      } else if (removed.busCode) {
        // try to find by busCode -> bus._id earlier; best-effort skip otherwise
        const maybe = await Bus.findOne({ busCode: removed.busCode }).lean();
        if (maybe && maybe._id) await BusSlot.deleteOne({ busId: maybe._id }).catch(() => { });
      }
    } catch (e) {
      console.warn('Failed to delete BusSlot for removed bus', e && e.message ? e.message : e);
    }


    return res.json({ success: true, id: removed._id || removed.id || id });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: 'Failed to delete bus', details: err.message });
  }
});

// Bulk actions: POST /api/buses/bulk  { action: 'delete'|'activate'|'cancel', ids: [] }
app.post('/api/buses/bulk', async (req, res) => {
  try {
    const { action, ids } = req.body;
    if (!Array.isArray(ids) || ids.length === 0) {
      return res.status(400).json({ error: 'ids array required' });
    }

    if (action === 'delete') {
      // split ids into valid ObjectIds and non-object strings
      const objectIds = ids.filter(i => mongoose.Types.ObjectId.isValid(i)).map(i => mongoose.Types.ObjectId(i));
      const stringIds = ids.filter(i => !mongoose.Types.ObjectId.isValid(i));

      const orClauses = [];
      if (objectIds.length) orClauses.push({ _id: { $in: objectIds } });
      if (stringIds.length) {
        orClauses.push({ busCode: { $in: stringIds } });
        orClauses.push({ id: { $in: stringIds } });
      }

      if (orClauses.length === 0) {
        return res.status(400).json({ error: 'No valid ids provided' });
      }

      await Bus.deleteMany({ $or: orClauses });
      return res.json({ success: true });
    } else if (action === 'activate' || action === 'cancel') {
      // normalize ids for updateMany: use _id when possible, otherwise match busCode/id
      const objectIds = ids.filter(i => mongoose.Types.ObjectId.isValid(i)).map(i => mongoose.Types.ObjectId(i));
      const stringIds = ids.filter(i => !mongoose.Types.ObjectId.isValid(i));

      const updateFilter = { $or: [] };
      if (objectIds.length) updateFilter.$or.push({ _id: { $in: objectIds } });
      if (stringIds.length) {
        updateFilter.$or.push({ busCode: { $in: stringIds } });
        updateFilter.$or.push({ id: { $in: stringIds } });
      }

      const status = action === 'activate' ? 'scheduled' : 'cancelled';
      await Bus.updateMany(updateFilter, { $set: { status, updatedAt: new Date() } });
      return res.json({ success: true });
    } else {
      return res.status(400).json({ error: 'Unknown action' });
    }
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: 'Bulk action failed', details: err.message });
  }
});

// New endpoint: add fake bookings (mark available seats as booked)
// POST /api/buses/:id/fake_bookings  body: { count: 1 }
app.post('/api/buses/:id/fake_bookings', async (req, res) => {
  try {
    const id = req.params.id;
    const count = Math.max(1, parseInt(req.body.count) || 1);
    const bus = await Bus.findById(id);
    if (!bus) return res.status(404).json({ error: 'Bus not found' });

    if (!Array.isArray(bus.seatMap) || bus.seatMap.length === 0) {
      return res.status(400).json({ error: 'No seat map available for this bus' });
    }

    // find available seats
    const availableSeatsIdx = [];
    for (let i = 0; i < bus.seatMap.length; i) {
      const s = bus.seatMap[i];
      if (!s.status || s.status === 'available') availableSeatsIdx.push(i);
    }
    if (availableSeatsIdx.length === 0) {
      return res.status(400).json({ error: 'No available seats left to book' });
    }

    // decide how many to book
    const toBook = Math.min(count, availableSeatsIdx.length);
    // simple shuffle
    for (let i = availableSeatsIdx.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [availableSeatsIdx[i], availableSeatsIdx[j]] = [availableSeatsIdx[j], availableSeatsIdx[i]];
    }

    for (let k = 0; k < toBook; k++) {
      const idx = availableSeatsIdx[k];
      bus.seatMap[idx].status = 'booked';
    }
    // update seatsAvailable
    const bookedNow = bus.seatMap.filter(s => s.status === 'booked').length;
    bus.seatsTotal = bus.seatMap.length;
    bus.seatsAvailable = Math.max(0, bus.seatsTotal - bookedNow);
    bus.updatedAt = new Date();
    await bus.save();
    return res.json(bus);
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: 'Failed to create fake bookings', details: err.message });
  }
});

// New: client-facing list endpoint for /api/client/buses
app.get('/api/client/buses', async (req, res) => {
  try {
    const page = Math.max(1, parseInt(req.query.page) || 1);
    const pageSize = Math.max(1, parseInt(req.query.pageSize) || 10);
    const from = req.query.from ? String(req.query.from).trim() : '';
    const to = req.query.to ? String(req.query.to).trim() : '';
    const departure = req.query.departure ? String(req.query.departure).trim() : '';
    const operator = req.query.operator ? String(req.query.operator).trim() : '';
    const status = req.query.status ? String(req.query.status).trim() : '';

    const filter = {};

    // small helper to escape user input for Regex
    const escapeRegex = (s = '') => String(s).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

    // from / to filtering: try to match code / city / name (case-insensitive, partial)
    if (from) {
      const re = new RegExp(escapeRegex(from), 'i');
      const numFrom = Number(from);
      const fromOr = [
        { 'routeFrom.code': re },
        { 'routeFrom.city': re },
        { 'routeFrom.name': re },
        { 'routeFrom.id': re },
        // exact matches (cover numeric-typed fields too)
        { 'routeFrom.code': from }
      ];
      if (!Number.isNaN(numFrom)) fromOr.push({ 'routeFrom.code': numFrom });
      filter.$and = filter.$and || [];
      filter.$and.push({ $or: fromOr });
    }

    if (to) {
      const re = new RegExp(escapeRegex(to), 'i');
      const numTo = Number(to);
      const toOr = [
        { 'routeTo.code': re },
        { 'routeTo.city': re },
        { 'routeTo.name': re },
        { 'routeTo.id': re },
        { 'routeTo.code': to }
      ];
      if (!Number.isNaN(numTo)) toOr.push({ 'routeTo.code': numTo });
      filter.$and = filter.$and || [];
      filter.$and.push({ $or: toOr });
    }

    if (operator) {
      filter['operator.id'] = operator;
    }
    if (status) {
      filter.status = status;
    }

    // departure: match any departureDate or departureAt inside the same day
    let depStart, depEnd;
    if (departure) {
      // Try to parse as YYYY-MM-DD robustly (create UTC day range)
      let dt = null;
      if (/^\d{4}-\d{2}-\d{2}$/.test(departure)) {
        // treat as date-only in UTC to avoid local TZ shifts
        dt = new Date(departure + 'T00:00:00Z');
      } else {
        dt = new Date(departure);
      }
      if (!isNaN(dt.getTime())) {
        // normalize to UTC day boundaries
        depStart = new Date(Date.UTC(dt.getUTCFullYear(), dt.getUTCMonth(), dt.getUTCDate(), 0, 0, 0, 0));
        depEnd = new Date(depStart);
        depEnd.setUTCDate(depEnd.getUTCDate() + 1);

        filter.$and = filter.$and || [];
        filter.$and.push({
          $or: [
            { departureAt: { $gte: depStart, $lt: depEnd } },
            { departureDates: { $elemMatch: { $gte: depStart, $lt: depEnd } } }
          ]
        });
      }
    }

    // safer logging: convert Date -> ISO string for readable logs
    const safeStringify = (obj) => JSON.stringify(obj, (k, v) => (v instanceof Date ? v.toISOString() : v));
    console.log('GET /api/client/buses - filter:', safeStringify(filter));
    if (depStart && depEnd) {
      console.log('departure range (UTC):', depStart.toISOString(), depEnd.toISOString());
    }

    // count and fetch with projection suitable for client listing
    let total = await Bus.countDocuments(filter);
    let buses = await Bus.find(filter)
      .sort({ departureAt: 1 })
      .skip((page - 1) * pageSize)
      .limit(pageSize)
      .select('busCode operator routeFrom routeTo departureAt departureDates arrivalAt arrivalDates adultPrice childPrice seatsAvailable seatsTotal duration busType status amenities seatMap')
      .lean();

    // normalize amenities for client responses
    buses = buses.map(b => {
      if (typeof b.amenities === 'string') b.amenities = b.amenities.split(',').map((s) => s.trim()).filter(Boolean);
      if (!Array.isArray(b.amenities)) b.amenities = [];
      // backfill adultPrice from legacy price if any doc still has old field
      if ((typeof b.adultPrice !== 'number' || isNaN(b.adultPrice)) && typeof b.price === 'number') {
        b.adultPrice = Number(b.price) || 0;
      }
      if (typeof b.childPrice !== 'number') b.childPrice = b.childPrice || 0;
      // remove legacy price key from response to client
      if (typeof b.price !== 'undefined') delete b.price;
      return b;
    });

    // If strict filter returned nothing but user provided from/to, try a relaxed fallback:
    if ((Array.isArray(buses) && buses.length === 0) && (from || to)) {
      const relaxedAnd = [];
      if (from) {
        const reFrom = new RegExp(escapeRegex(from), 'i');
        relaxedAnd.push({
          $or: [
            { 'routeFrom.code': reFrom }, { 'routeFrom.city': reFrom }, { 'routeFrom.name': reFrom },
            { 'routeTo.code': reFrom }, { 'routeTo.city': reFrom }, { 'routeTo.name': reFrom }
          ]
        });
      }
      if (to) {
        const reTo = new RegExp(escapeRegex(to), 'i');
        relaxedAnd.push({
          $or: [
            { 'routeTo.code': reTo }, { 'routeTo.city': reTo }, { 'routeTo.name': reTo },
            { 'routeFrom.code': reTo }, { 'routeFrom.city': reTo }, { 'routeFrom.name': reTo }
          ]
        });
      }
      if (depStart && depEnd) {
        relaxedAnd.push({
          $or: [
            { departureAt: { $gte: depStart, $lt: depEnd } },
            { departureDates: { $elemMatch: { $gte: depStart, $lt: depEnd } } }
          ]
        });
      }
      const relaxedFilter = relaxedAnd.length ? { $and: relaxedAnd } : {};
      console.log('relaxed fallback filter:', safeStringify(relaxedFilter));
      total = await Bus.countDocuments(relaxedFilter);
      buses = await Bus.find(relaxedFilter)
        .sort({ departureAt: 1 })
        .skip((page - 1) * pageSize)
        .limit(pageSize)
        .select('busCode operator routeFrom routeTo departureAt departureDates arrivalAt arrivalDates price seatsAvailable seatsTotal duration busType status amenities seatMap')
        .lean();

      // mark response so client knows a relaxed search was used
      return res.json({
        data: buses,
        pagination: { total, current: page, pageSize },
        relaxed: true
      });
    }

    return res.json({
      data: buses,
      pagination: { total, current: page, pageSize }
    });
  } catch (err) {
    console.error('GET /api/client/buses error:', err);
    return res.status(500).json({ error: 'Failed to fetch client buses', details: err.message });
  }
});

app.post('/api/buses/:id/slots/reserve', async (req, res) => {
  try {
    const busIdParam = req.params.id;
    const { dateIso: rawDate, seats, count = 0, reservationId, orderNumber, customerId } = req.body || {};
    const dateIso = normalizeDateIso(rawDate);
    if (!dateIso) return res.status(400).json({ error: 'dateIso_required' });
    if ((!Array.isArray(seats) || seats.length === 0) && (!Number.isInteger(count) || count <= 0)) {
      return res.status(400).json({ error: 'seats_or_count_required' });
    }

    // resolve bus
    let bus = null;
    if (mongoose.Types.ObjectId.isValid(busIdParam)) bus = await Bus.findById(busIdParam).lean();
    if (!bus) bus = await Bus.findOne({ busCode: busIdParam }).lean();
    if (!bus) return res.status(404).json({ error: 'bus_not_found' });

    // upsert slot doc if missing
    let slotDoc = await BusSlot.findOne({ busId: bus._id });
    if (!slotDoc) {
      // create initial from bus
      const init = (Array.isArray(bus.departureDates) ? bus.departureDates : [bus.departureAt]).map(d => {
        const iso = normalizeDateIso(d);
        if (!iso) return null;
        const seatmap = buildSeatmapFillFromBus(bus);
        const seatsTotal = (bus.seatMap || []).length || Number(bus.seatsTotal || 0);
        const log = (seatmap || []).filter(s => s.status === 'booked').map(s => ({
          seatId: s.seatId,
          reservationId: s.reservationId || null,
          orderNumber: null,
          customerId: null,
          ts: new Date()
        }));
        const seatReserved = log.length;
        return {
          dateIso: iso,
          seatmapFill: seatmap,
          seatsTotal,
          seatReserved,
          seatsAvailable: Math.max(0, seatsTotal - seatReserved),
          logSeatBooked: log
        };
      }).filter(Boolean);
      slotDoc = new BusSlot({ busId: bus._id, dateBookings: init });
      await slotDoc.save();

    }

    // transactionally update the dateBooking entry
    const session = await mongoose.startSession();
    session.startTransaction();
    try {
      const doc = await BusSlot.findOne({ busId: bus._id }).session(session);
      const dbEntry = (doc.dateBookings || []).find(d => d.dateIso === dateIso);
      if (!dbEntry) throw new Error('date_slot_not_found');

      const assigned = [];
      const rid = reservationId || `R_${Date.now().toString().slice(-6)}`;
      if (Array.isArray(seats) && seats.length) {
        // atomic booking with idempotency: treat seats already booked by SAME reservationId as OK
        const seatsRequested = seats.map(s => String(s || '').trim().toUpperCase());
        const seatMapByUpper = new Map(dbEntry.seatmapFill.map((s, i) => [String(s.seatId || s.label || '').toUpperCase(), i]));

        const unavailable = [];
        const toBookIdx = [];
        const alreadyAssigned = [];

        for (const sidUpper of seatsRequested) {
          const idx = seatMapByUpper.get(sidUpper);
          if (typeof idx === 'undefined') {
            unavailable.push({ seat: sidUpper, reason: 'not_found' });
            continue;
          }
          const cur = dbEntry.seatmapFill[idx];
          // available -> will book
          if (!cur.status || cur.status === 'available') {
            toBookIdx.push(idx);
            continue;
          }
          // already booked/blocked
          const curRid = cur.reservationId || null;
          // if incoming reservationId matches existing, consider idempotent success
          if (reservationId && curRid && String(curRid) === String(reservationId)) {
            alreadyAssigned.push(cur);
            continue;
          }
          // otherwise it's a conflict => unavailable
          unavailable.push({ seat: sidUpper, reason: 'not_available', currentStatus: cur.status, reservationId: curRid });
        }

        if (unavailable.length) {
          // abort transaction and inform caller which seats conflict
          await session.abortTransaction();
          session.endSession();
          return res.status(409).json({ success: false, error: 'seats_unavailable', details: unavailable });
        }

        // perform booking for all requested seats that were available
        for (const idx of toBookIdx) {
          const seatObj = dbEntry.seatmapFill[idx];
          seatObj.status = 'booked';
          seatObj.reservationId = reservationId || rid;
          // avoid duplicate log if same reservation already present
          const existsLog = (dbEntry.logSeatBooked || []).some(l => String(l.seatId) === String(seatObj.seatId) && String(l.reservationId) === String(seatObj.reservationId) && l.status === 'confirm');
          if (!existsLog) {
            dbEntry.logSeatBooked.push({
              seatId: seatObj.seatId,
              reservationId: seatObj.reservationId,
              orderNumber: orderNumber || null,
              customerId: customerId || null,
              status: 'confirm',
              ts: new Date()
            });
          }
          dbEntry.seatReserved = (dbEntry.seatReserved || 0) + 1;
          assigned.push(seatObj);
        }
        // include seats already booked by same reservationId (idempotent)
        for (const s of alreadyAssigned) assigned.push(s);
      } else {
        for (const s of dbEntry.seatmapFill) {
          if (assigned.length >= count) break;
          if (!s.status || s.status === 'available') {
            s.status = 'booked';
            s.reservationId = rid;
            dbEntry.logSeatBooked.push({
              seatId: s.seatId,
              reservationId: rid,
              orderNumber: orderNumber || null,
              customerId: customerId || null,
              status: 'confirm',
              ts: new Date()
            });
            dbEntry.seatReserved = (dbEntry.seatReserved || 0) + 1;
            assigned.push(s);
          }
        }
      }

      dbEntry.seatsTotal = dbEntry.seatmapFill.length;
      dbEntry.seatReserved = dbEntry.seatReserved || dbEntry.seatmapFill.filter(s => s.status === 'booked').length;
      dbEntry.seatsAvailable = Math.max(0, dbEntry.seatsTotal - dbEntry.seatReserved);

      await doc.save({ session });
      await session.commitTransaction();
      session.endSession();

      return res.json({ success: true, reservationId: rid, seats: assigned, seatsAvailable: dbEntry.seatsAvailable });
    } catch (err) {
      await session.abortTransaction();
      session.endSession();
      throw err;
    }
  } catch (err) {
    console.error('POST /api/buses/:id/slots/reserve error', err);
    return res.status(500).json({ error: 'reserve_failed', details: err.message });
  }
});

// POST /api/buses/:id/slots/release  body: { dateIso, seats?: string[], reservationId?: string, count?: number, orderNumber }
app.post('/api/buses/:id/slots/release', async (req, res) => {
  try {
    const busIdParam = req.params.id;
    const { dateIso: rawDate, seats, reservationId, count = 0, orderNumber } = req.body || {};
    const dateIso = normalizeDateIso(rawDate);
    if (!dateIso) return res.status(400).json({ error: 'dateIso_required' });
    if ((!Array.isArray(seats) || seats.length === 0) && !reservationId && (!Number.isInteger(count) || count <= 0)) {
      return res.status(400).json({ error: 'seats_or_reservationId_or_count_required' });
    }

    let bus = null;
    if (mongoose.Types.ObjectId.isValid(busIdParam)) bus = await Bus.findById(busIdParam).lean();
    if (!bus) bus = await Bus.findOne({ busCode: busIdParam }).lean();
    if (!bus) return res.status(404).json({ error: 'bus_not_found' });

    const slotDoc = await BusSlot.findOne({ busId: bus._id });
    if (!slotDoc) return res.status(404).json({ error: 'slots_not_found' });

    const session = await mongoose.startSession();
    session.startTransaction();
    try {
      const doc = await BusSlot.findOne({ busId: bus._id }).session(session);
      const dbEntry = (doc.dateBookings || []).find(d => d.dateIso === dateIso);
      if (!dbEntry) throw new Error('date_slot_not_found');

      const released = [];
      if (Array.isArray(seats) && seats.length) {
        for (const sid of seats) {
          const idx = dbEntry.seatmapFill.findIndex(s => (s.seatId === sid || s.label === sid));
          if (idx === -1) continue;
          if (!dbEntry.seatmapFill[idx].status || dbEntry.seatmapFill[idx].status === 'available') continue;
          // mark matching log entries as cancelled (keep record)
          for (const logEntry of dbEntry.logSeatBooked || []) {
            if (logEntry.seatId === dbEntry.seatmapFill[idx].seatId && (!orderNumber || String(logEntry.orderNumber) === String(orderNumber))) {
              if (logEntry.status === 'confirm') {
                logEntry.status = 'cancel';
                logEntry.cancelledAt = new Date();
              }
            }
          }
          dbEntry.seatmapFill[idx].status = 'available';
          dbEntry.seatmapFill[idx].reservationId = null;
          released.push(dbEntry.seatmapFill[idx]);
        }
      } else if (reservationId) {
        for (const s of dbEntry.seatmapFill) {
          if (String(s.reservationId) === String(reservationId)) {
            // mark log entries for this reservation as cancelled (keep them)
            for (const logEntry of dbEntry.logSeatBooked || []) {
              if (String(logEntry.reservationId) === String(reservationId) && logEntry.status === 'confirm') {
                logEntry.status = 'cancel';
                logEntry.cancelledAt = new Date();
              }
            }
            s.status = 'available';
            s.reservationId = null;
            released.push(s);
          }
        }
      } else if (Number.isInteger(count) && count > 0) {
        for (const s of dbEntry.seatmapFill) {
          if (released.length >= count) break;
          if (s.reservationId) {
            // remove first matching log entry for this seat
            // mark first confirmed log for this seat as cancelled
            const idxLog = (dbEntry.logSeatBooked || []).findIndex(l => l.seatId === s.seatId && l.status === 'confirm');
            if (idxLog !== -1) {
              dbEntry.logSeatBooked[idxLog].status = 'cancel';
              dbEntry.logSeatBooked[idxLog].cancelledAt = new Date();
              s.status = 'available';
              s.reservationId = null;
              released.push(s);
            }
          }
        }
      }

      // dbEntry.seatsTotal = dbEntry.seatmapFill.length;
      // dbEntry.seatReserved = dbEntry.seatReserved || dbEntry.seatmapFill.filter(s => s.status === 'booked').length;
      // dbEntry.seatsAvailable = Math.max(0, dbEntry.seatsTotal - dbEntry.seatReserved);
      // recompute reserved/available based on confirmed log entries and seatmap
      dbEntry.seatsTotal = dbEntry.seatmapFill.length;
      const reservedFromLog = (dbEntry.logSeatBooked || []).filter(l => l.status === 'confirm').length;
      const reservedFromMap = dbEntry.seatmapFill.filter(s => s.status === 'booked').length;
      dbEntry.seatReserved = Math.max(reservedFromLog, reservedFromMap);
      dbEntry.seatsAvailable = Math.max(0, dbEntry.seatsTotal - dbEntry.seatReserved);
      await doc.save({ session });
      await session.commitTransaction();
      session.endSession();

      return res.json({ success: true, released, seatsAvailable: dbEntry.seatsAvailable });
    } catch (err) {
      await session.abortTransaction();
      session.endSession();
      throw err;
    }
  } catch (err) {
    console.error('POST /api/buses/:id/slots/release error', err);
    return res.status(500).json({ error: 'release_failed', details: err.message });
  }
});

app.get('/api/buses/:id/slots/:date', async (req, res) => {
  try {
    const busIdParam = req.params.id;
    const dateParam = req.params.date;
    const dateIso = normalizeDateIso(dateParam);
    if (!dateIso) return res.status(400).json({ error: 'invalid_date' });

    // resolve bus by _id or busCode
    let bus = null;
    if (mongoose.Types.ObjectId.isValid(busIdParam)) bus = await Bus.findById(busIdParam).lean();
    if (!bus) bus = await Bus.findOne({ busCode: busIdParam }).lean();
    if (!bus) return res.status(404).json({ error: 'bus_not_found' });

    // ensure slot doc exists: create initial if missing (same logic as reserve handler)
    let slotDoc = await BusSlot.findOne({ busId: bus._id });
    if (!slotDoc) {
      const inits = (Array.isArray(bus.departureDates) ? bus.departureDates : [bus.departureAt]).map(d => {
        const iso = normalizeDateIso(d);
        if (!iso) return null;
        const seatmap = buildSeatmapFillFromBus(bus);
        const seatsTotal = (bus.seatMap || []).length || Number(bus.seatsTotal || 0);
        const log = (seatmap || []).filter(s => s.status === 'booked').map(s => ({
          seatId: s.seatId,
          reservationId: s.reservationId || null,
          orderNumber: null,
          customerId: null,
          ts: new Date()
        }));
        const seatReserved = log.length;
        return {
          dateIso: iso,
          seatmapFill: seatmap,
          seatsTotal,
          seatReserved,
          seatsAvailable: Math.max(0, seatsTotal - seatReserved),
          logSeatBooked: log
        };
      }).filter(Boolean);
      slotDoc = new BusSlot({ busId: bus._id, dateBookings: inits });
      await slotDoc.save();
      // reload as lean for response
      slotDoc = await BusSlot.findOne({ busId: bus._id }).lean();
    } else {
      // ensure fresh lean doc for read-only response
      slotDoc = await BusSlot.findOne({ busId: bus._id }).lean();
    }

    const dbEntry = (slotDoc.dateBookings || []).find(d => d.dateIso === dateIso);
    if (!dbEntry) return res.status(404).json({ error: 'date_slot_not_found' });

    // respond shape compatible with client: { slot: dbEntry }
    return res.json({ slot: dbEntry });
  } catch (err) {
    console.error('GET /api/buses/:id/slots/:date error:', err && err.message ? err.message : err);
    return res.status(500).json({ error: 'failed', details: err.message });
  }
});
/* ----------------- Promotions: Schema + CRUD API ----------------- */

const PromotionSchema = new mongoose.Schema({
  code: { type: String, uppercase: true, sparse: true, index: true }, // allow null, index for lookup
  title: { type: String, required: true },
  description: { type: String, default: "" },
  type: { type: String, enum: ["percent", "fixed"], required: true },
  value: { type: Number, required: true, min: 0 },
  minSpend: { type: Number, default: 0, min: 0 },
  maxUses: { type: Number, default: 0, min: 0 }, // 0 = unlimited
  usedCount: { type: Number, default: 0, min: 0 },
  appliesTo: { type: [String], default: [] },
  validFrom: { type: Date, required: true },
  validTo: { type: Date, required: true },
  active: { type: Boolean, default: true },
  requireCode: { type: Boolean, default: true },
  autoApply: { type: Boolean, default: false },
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now }
}, { collection: 'promotions' });

// Basic validation/normalization
PromotionSchema.pre('validate', function (next) {
  try {
    // normalize code
    if (this.code && typeof this.code === 'string') {
      this.code = this.code.trim().toUpperCase();
      if (!/^[A-Z0-9]{3,20}$/.test(this.code)) {
        return next(new Error('Invalid promotion code format (A-Z,0-9, 3-20 chars)'));
      }
    } else {
      // if autoApply = false and requireCode true, code must exist
      if (!this.autoApply && this.requireCode) {
        return next(new Error('Code required when requireCode is true and autoApply is false'));
      }
    }

    // date sanity
    if (this.validFrom && this.validTo) {
      if (new Date(this.validTo) <= new Date(this.validFrom)) {
        return next(new Error('validTo must be after validFrom'));
      }
    }

    // clamp numeric fields
    if (typeof this.value !== 'number' || this.value < 0) this.value = Math.max(0, Number(this.value) || 0);
    if (typeof this.minSpend !== 'number' || this.minSpend < 0) this.minSpend = Math.max(0, Number(this.minSpend) || 0);
    if (typeof this.maxUses !== 'number' || this.maxUses < 0) this.maxUses = Math.max(0, Number(this.maxUses) || 0);
    if (typeof this.usedCount !== 'number' || this.usedCount < 0) this.usedCount = Math.max(0, Number(this.usedCount) || 0);

    // ensures appliesTo is array of trimmed strings
    if (typeof this.appliesTo === 'string') {
      this.appliesTo = this.appliesTo.split(',').map(s => String(s).trim()).filter(Boolean);
    }
    if (!Array.isArray(this.appliesTo)) this.appliesTo = [];

    return next();
  } catch (err) {
    return next(err);
  }
});

PromotionSchema.pre('save', function (next) {
  this.updatedAt = new Date();
  next();
});

const Promotion = mongoose.model('Promotion', PromotionSchema);

// List promotions with pagination & filters
app.get('/api/promotions', async (req, res) => {
  try {
    const page = Math.max(1, parseInt(req.query.page) || 1);
    const pageSize = Math.max(1, parseInt(req.query.pageSize) || 20);
    const search = req.query.search ? String(req.query.search).trim() : '';
    const status = req.query.status ? String(req.query.status).trim() : 'all'; // active/inactive/expired/used_up/all
    const type = req.query.type ? String(req.query.type).trim() : 'all';
    const appliesTo = req.query.appliesTo ? String(req.query.appliesTo).trim() : 'all';

    const filter = {};

    if (search) {
      const re = new RegExp(search, 'i');
      filter.$or = [{ code: re }, { title: re }, { description: re }];
    }

    if (type !== 'all') {
      filter.type = type;
    }

    if (appliesTo !== 'all') {
      // match promotions whose appliesTo array contains the requested value
      filter.appliesTo = appliesTo;
    }

    // status handling requires computing against dates and usedCount
    const now = new Date();
    if (status !== 'all') {
      if (status === 'active') {
        filter.active = true;
        filter.validFrom = { $lte: now };
        filter.validTo = { $gte: now };
        // used_up check: either maxUses === 0 (infinite) or usedCount < maxUses
        filter.$or = filter.$or || [];
        filter.$or.push({ maxUses: 0 }, { $expr: { $lt: ["$usedCount", "$maxUses"] } });
      } else if (status === 'inactive') {
        filter.active = false;
      } else if (status === 'expired') {
        filter.validTo = { $lt: now };
      } else if (status === 'used_up') {
        filter.$expr = { $and: [{ $gt: ["$maxUses", 0] }, { $gte: ["$usedCount", "$maxUses"] }] };
      }
    }

    // --- MISSING DB QUERY FIX: compute total and fetch data ---
    const total = await Promotion.countDocuments(filter);
    let data = await Promotion.find(filter)
      .sort({ createdAt: -1 })
      .skip((page - 1) * pageSize)
      .limit(pageSize)
      .lean();

    // Normalization & derived fields for client convenience
    data = data.map(p => {
      const validFrom = p.validFrom ? new Date(p.validFrom) : null;
      const validTo = p.validTo ? new Date(p.validTo) : null;
      const isExpired = validTo ? validTo < now : false;
      const isActiveNow = !!p.active && validFrom && validTo ? (validFrom <= now && now <= validTo) : !!p.active;
      const isUsedUp = (p.maxUses && p.maxUses > 0) ? (p.usedCount >= p.maxUses) : false;
      return {
        ...p,
        code: p.code && p.code.trim().length ? p.code.trim().toUpperCase() : null,
        validFrom: validFrom ? validFrom.toISOString() : null,
        validTo: validTo ? validTo.toISOString() : null,
        isExpired,
        isActiveNow,
        isUsedUp
      };
    });

    return res.json({ data, pagination: { total, current: page, pageSize } });
  } catch (err) {
    console.error('GET /api/promotions error:', err);
    return res.status(500).json({ error: 'Failed to fetch promotions', details: err.message });
  }
});

// Get single promotion by id or code
app.get('/api/promotions/:id', async (req, res) => {
  try {
    const id = req.params.id;
    let item = null;
    if (mongoose.Types.ObjectId.isValid(id)) {
      item = await Promotion.findById(id).lean();
    }
    if (!item) {
      item = await Promotion.findOne({ code: id.toUpperCase() }).lean();
    }
    if (!item) return res.status(404).json({ error: 'Promotion not found' });
    return res.json(item);
  } catch (err) {
    console.error('GET /api/promotions/:id error:', err);
    return res.status(500).json({ error: 'Failed to fetch promotion', details: err.message });
  }
});

// Create promotion
app.post('/api/promotions', async (req, res) => {
  try {
    const payload = req.body || {};

    // normalize
    if (payload.code) payload.code = String(payload.code).trim().toUpperCase();
    payload.appliesTo = Array.isArray(payload.appliesTo) ? payload.appliesTo.map(String) : (payload.appliesTo ? [String(payload.appliesTo)] : []);
    payload.requireCode = !!payload.requireCode;
    payload.autoApply = !!payload.autoApply;

    // parse dates
    if (payload.validFrom) payload.validFrom = new Date(payload.validFrom);
    if (payload.validTo) payload.validTo = new Date(payload.validTo);

    // code uniqueness: only if code provided
    if (payload.code) {
      const exists = await Promotion.findOne({ code: payload.code }).lean();
      if (exists) return res.status(400).json({ error: 'Promotion code already exists' });
    } else {
      if (!payload.autoApply && payload.requireCode) {
        return res.status(400).json({ error: 'Code is required when requireCode is true and autoApply is false' });
      }
    }

    const promo = new Promotion(payload);
    await promo.save();
    return res.status(201).json(promo);
  } catch (err) {
    console.error('POST /api/promotions error:', err);
    return res.status(400).json({ error: 'Failed to create promotion', details: err.message });
  }
});

// Update promotion
app.put('/api/promotions/:id', async (req, res) => {
  try {
    const id = req.params.id;
    const payload = req.body || {};

    // normalize
    if (payload.code) payload.code = String(payload.code).trim().toUpperCase();
    if (payload.appliesTo && !Array.isArray(payload.appliesTo)) payload.appliesTo = [String(payload.appliesTo)];
    if (payload.validFrom) payload.validFrom = new Date(payload.validFrom);
    if (payload.validTo) payload.validTo = new Date(payload.validTo);
    if (typeof payload.requireCode !== 'undefined') payload.requireCode = !!payload.requireCode;
    if (typeof payload.autoApply !== 'undefined') payload.autoApply = !!payload.autoApply;

    // If updating code, ensure uniqueness
    if (payload.code) {
      const existing = await Promotion.findOne({ code: payload.code, _id: { $ne: id } }).lean();
      if (existing) return res.status(400).json({ error: 'Promotion code already exists' });
    } else {
      // if turning off autoApply and requireCode true but code missing => validation error
      if (payload.requireCode && payload.autoApply === false && !payload.code) {
        // allow if existing document already has code
        const current = mongoose.Types.ObjectId.isValid(id) ? await Promotion.findById(id).lean() : null;
        if (!current || !current.code) {
          return res.status(400).json({ error: 'Code required when requireCode is true and autoApply false' });
        }
      }
    }

    payload.updatedAt = new Date();
    const updated = await Promotion.findByIdAndUpdate(id, payload, { new: true, runValidators: true }).lean();
    if (!updated) return res.status(404).json({ error: 'Promotion not found' });
    return res.json(updated);
  } catch (err) {
    console.error('PUT /api/promotions/:id error:', err);
    return res.status(400).json({ error: 'Failed to update promotion', details: err.message });
  }
});

// Delete promotion
app.delete('/api/promotions/:id', async (req, res) => {
  try {
    const id = req.params.id;
    let removed = null;

    if (mongoose.Types.ObjectId.isValid(id)) {
      removed = await Promotion.findByIdAndDelete(id).lean();
    }
    if (!removed) {
      removed = await Promotion.findOneAndDelete({ code: id.toUpperCase() }).lean();
    }
    if (!removed) return res.status(404).json({ error: 'Promotion not found' });
    return res.json({ success: true, id: removed._id || removed.code || id });
  } catch (err) {
    console.error('DELETE /api/promotions/:id error:', err);
    return res.status(500).json({ error: 'Failed to delete promotion', details: err.message });
  }
});

// Bulk actions for promotions: POST /api/promotions/bulk { action, ids }
app.post('/api/promotions/bulk', async (req, res) => {
  try {
    const { action, ids } = req.body;
    if (!Array.isArray(ids) || ids.length === 0) return res.status(400).json({ error: 'ids array required' });

    const objectIds = ids.filter(i => mongoose.Types.ObjectId.isValid(i)).map(i => mongoose.Types.ObjectId(i));
    const stringIds = ids.filter(i => !mongoose.Types.ObjectId.isValid(i)).map(i => String(i).toUpperCase());

    const orClauses = [];
    if (objectIds.length) orClauses.push({ _id: { $in: objectIds } });
    if (stringIds.length) orClauses.push({ code: { $in: stringIds } });

    if (orClauses.length === 0) return res.status(400).json({ error: 'No valid ids provided' });

    if (action === 'delete') {
      await Promotion.deleteMany({ $or: orClauses });
      return res.json({ success: true });
    } else if (action === 'activate' || action === 'deactivate') {
      const active = action === 'activate';
      await Promotion.updateMany({ $or: orClauses }, { $set: { active, updatedAt: new Date() } });
      return res.json({ success: true });
    } else {
      return res.status(400).json({ error: 'Unknown action' });
    }
  } catch (err) {
    console.error('POST /api/promotions/bulk error:', err);
    return res.status(500).json({ error: 'Bulk action failed', details: err.message });
  }
});

app.post('/api/promotions/validate', async (req, res) => {
  try {
    const { code, serviceType = 'all', amount } = req.body || {};
    if (!code || typeof code !== 'string') {
      return res.status(400).json({ ok: false, error: 'code_required' });
    }
    const numericAmount = Number(amount || 0);
    if (!Number.isFinite(numericAmount) || numericAmount < 0) {
      return res.status(400).json({ ok: false, error: 'invalid_amount' });
    }

    const codeNorm = String(code).trim().toUpperCase();
    const promo = await Promotion.findOne({ code: codeNorm }).lean();
    if (!promo) return res.status(404).json({ ok: false, error: 'promo_not_found' });

    const now = new Date();
    if (!promo.active) return res.status(400).json({ ok: false, error: 'promo_inactive' });
    if (promo.validFrom && new Date(promo.validFrom) > now) return res.status(400).json({ ok: false, error: 'promo_not_started' });
    if (promo.validTo && new Date(promo.validTo) < now) return res.status(400).json({ ok: false, error: 'promo_expired' });
    if (promo.maxUses && promo.maxUses > 0 && promo.usedCount >= promo.maxUses) return res.status(400).json({ ok: false, error: 'promo_used_up' });

    const applies = Array.isArray(promo.appliesTo) ? promo.appliesTo.map(String) : [];
    // robust service-type check: normalize to lowercase and accept plural/singular variants
    const appliesNorm = applies.map(a => String(a).toLowerCase());
    const svc = String(serviceType || 'all').toLowerCase();
    const stripS = (s) => s.replace(/s$/, '');
    const matchesApply =
      appliesNorm.includes('all') ||
      appliesNorm.includes(svc) ||
      appliesNorm.includes(stripS(svc)) ||
      appliesNorm.map(stripS).includes(stripS(svc));
    if (!matchesApply) {
      console.log('Promotion not applicable for serviceType:', serviceType, 'promo.appliesTo=', applies);
      return res.status(400).json({ ok: false, error: 'not_applicable_service' });
    }

    // treat 'amount' as eligible base (client should pass eligible amount for subset if needed)
    const eligibleAmount = numericAmount;
    if (eligibleAmount < (Number(promo.minSpend || 0))) {
      return res.status(400).json({
        ok: false,
        error: 'min_spend_not_met',
        requiredMinSpend: Number(promo.minSpend || 0),
        eligibleAmount
      });
    }

    // compute discount
    let discount = 0;
    if (promo.type === 'percent') {
      discount = Math.floor(eligibleAmount * (Number(promo.value || 0) / 100));
    } else {
      discount = Number(promo.value || 0);
    }

    // clamp discount to eligible amount
    discount = Math.max(0, Math.min(discount, eligibleAmount));

    // honor optional maxDiscount if present on promo document
    if (typeof promo.maxDiscount === 'number' && promo.maxDiscount > 0) {
      discount = Math.min(discount, promo.maxDiscount);
    }

    const newTotal = Math.max(0, Math.round(eligibleAmount - discount));

    return res.json({
      ok: true,
      promotion: {
        id: promo._id,
        code: promo.code || null,
        title: promo.title,
        type: promo.type,
        value: promo.value,
        minSpend: promo.minSpend,
        appliesTo: promo.appliesTo,
      },
      eligibleAmount,
      discount,
      newTotal
    });
  } catch (err) {
    console.error('POST /api/promotions/validate error:', err);
    return res.status(500).json({ ok: false, error: 'server_error', details: err.message });
  }
});




// api cho bài viết
// ----------------- Articles (News) API -----------------
const ArticleSchema = new mongoose.Schema({
  title: { type: String, required: true, trim: true },
  slug: { type: String, required: true, trim: true, lowercase: true, index: true },
  category: { type: String, default: "company" },
  status: { type: String, enum: ["published", "unpublished"], default: "unpublished" },
  author: { id: String, name: String, avatar: String },
  summary: { type: String, default: "" },
  content: { type: String, default: "" },
  featured: { type: Boolean, default: false },
  heroImage: { type: String, default: null },
  publishedAt: { type: Date, default: null },
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now },
  views: { type: Number, default: 0 },
}, { collection: 'articles' });

ArticleSchema.pre('save', function (next) {
  try {
    this.updatedAt = new Date();
    if (!this.createdAt) this.createdAt = this.updatedAt;
    if (this.status === 'published' && !this.publishedAt) this.publishedAt = new Date();
    // normalize slug
    if (this.slug && typeof this.slug === 'string') this.slug = String(this.slug).toLowerCase().trim();
    return next();
  } catch (err) {
    return next(err);
  }
});

const Article = mongoose.model('Article', ArticleSchema);

// List articles: GET /api/admin/news
app.get('/api/admin/news', async (req, res) => {
  try {
    const page = Math.max(1, parseInt(req.query.page) || 1);
    const limit = Math.max(1, parseInt(req.query.limit) || 10);
    const q = req.query.q ? String(req.query.q).trim() : '';
    const status = req.query.status ? String(req.query.status) : 'all';
    const category = req.query.category ? String(req.query.category) : 'all';
    // support featured filter: ?featured=true or ?featured=false
    const featuredParam = typeof req.query.featured !== 'undefined' ? String(req.query.featured).toLowerCase() : null;
    const filter = {};
    if (q) {
      const re = new RegExp(q, 'i');
      filter.$or = [{ title: re }, { summary: re }, { content: re }, { slug: re }];
    }
    if (status !== 'all') filter.status = status;
    if (category !== 'all') filter.category = category;
    if (featuredParam === 'true') filter.featured = true;
    if (featuredParam === 'false') filter.featured = false;
    const total = await Article.countDocuments(filter);
    const data = await Article.find(filter)
      .sort({ createdAt: -1 })
      .skip((page - 1) * limit)
      .limit(limit)
      .lean();

    return res.json({ data, pagination: { total, current: page, pageSize: limit } });
  } catch (err) {
    console.error('GET /api/admin/news error:', err);
    return res.status(500).json({ error: 'Failed to fetch articles', details: err.message });
  }
});

// Get single article by id: GET /api/admin/news/:id
app.get('/api/admin/news/:id', async (req, res) => {
  try {
    const id = req.params.id;
    let item = null;
    if (mongoose.Types.ObjectId.isValid(id)) {
      item = await Article.findById(id).lean();
    }
    if (!item) {
      // fallback to slug lookup
      item = await Article.findOne({ slug: id }).lean();
    }
    if (!item) return res.status(404).json({ error: 'Article not found' });
    return res.json(item);
  } catch (err) {
    console.error('GET /api/admin/news/:id error:', err);
    return res.status(500).json({ error: 'Failed to fetch article', details: err.message });
  }
});

// Create article: POST /api/admin/news
app.post('/api/admin/news', async (req, res) => {
  try {
    const payload = req.body || {};
    if (!payload.title || !payload.slug) return res.status(400).json({ error: 'title_and_slug_required' });

    // process embedded images in content before creating
    if (payload.content && typeof payload.content === 'string') {
      try {
        payload.content = await uploadEmbeddedImagesInHtml(payload.content, { folder: 'articles', maxBytes: 10 * 1024 * 1024 });
      } catch (err) {
        console.error('Failed processing embedded images:', err);
        return res.status(400).json({ error: 'embedded_image_processing_failed', details: err.message });
      }
    }

    // ensure unique slug
    const slugNorm = String(payload.slug).toLowerCase().trim();
    const exists = await Article.findOne({ slug: slugNorm }).lean();
    if (exists) return res.status(400).json({ error: 'slug_already_exists' });

    const article = new Article({
      title: String(payload.title).trim(),
      slug: slugNorm,
      category: payload.category || 'company',
      status: payload.status === 'published' ? 'published' : 'unpublished',
      author: payload.author || { id: 'system', name: 'Admin', avatar: '' },
      summary: payload.summary || '',
      content: payload.content || '',
      featured: !!payload.featured,
      heroImage: payload.heroImage || null,
      publishedAt: payload.status === 'published' ? (payload.publishedAt ? new Date(payload.publishedAt) : new Date()) : null,
    });

    await article.save();
    return res.status(201).json(article);
  } catch (err) {
    console.error('POST /api/admin/news error:', err);
    return res.status(400).json({ error: 'Failed to create article', details: err.message });
  }
});

// Update article: PUT /api/admin/news/:id
app.put('/api/admin/news/:id', async (req, res) => {
  try {
    const id = req.params.id;
    const payload = req.body || {};

    // process embedded images in content before updating
    if (payload.content && typeof payload.content === 'string') {
      try {
        payload.content = await uploadEmbeddedImagesInHtml(payload.content, { folder: 'articles', maxBytes: 10 * 1024 * 1024 });
      } catch (err) {
        console.error('Failed processing embedded images:', err);
        return res.status(400).json({ error: 'embedded_image_processing_failed', details: err.message });
      }
    }

    if (payload.slug) payload.slug = String(payload.slug).toLowerCase().trim();

    // check slug uniqueness if slug changed
    if (payload.slug && mongoose.Types.ObjectId.isValid(id)) {
      const other = await Article.findOne({ slug: payload.slug, _id: { $ne: id } }).lean();
      if (other) return res.status(400).json({ error: 'slug_already_exists' });
    } else if (payload.slug) {
      const other = await Article.findOne({ slug: payload.slug }).lean();
      if (other && String(other._id) !== id) return res.status(400).json({ error: 'slug_already_exists' });
    }

    if (payload.status === 'published' && !payload.publishedAt) payload.publishedAt = new Date();
    payload.updatedAt = new Date();

    const updated = await Article.findByIdAndUpdate(id, payload, { new: true, runValidators: true }).lean();
    if (!updated) return res.status(404).json({ error: 'Article not found' });
    return res.json(updated);
  } catch (err) {
    console.error('PUT /api/admin/news/:id error:', err);
    return res.status(400).json({ error: 'Failed to update article', details: err.message });
  }
});

// Delete article: DELETE /api/admin/news/:id
app.delete('/api/admin/news/:id', async (req, res) => {
  try {
    const id = req.params.id;
    let removed = null;
    if (mongoose.Types.ObjectId.isValid(id)) {
      removed = await Article.findByIdAndDelete(id).lean();
    }
    if (!removed) {
      removed = await Article.findOneAndDelete({ slug: id }).lean();
    }
    if (!removed) return res.status(404).json({ error: 'Article not found' });
    return res.json({ success: true, id: removed._id || id });
  } catch (err) {
    console.error('DELETE /api/admin/news/:id error:', err);
    return res.status(500).json({ error: 'Failed to delete article', details: err.message });
  }
});

// Bulk actions for admin: POST /api/admin/news/bulk { action, ids }
app.post('/api/admin/news/bulk', async (req, res) => {
  try {
    const { action, ids } = req.body;
    if (!Array.isArray(ids) || ids.length === 0) return res.status(400).json({ error: 'ids_required' });

    if (action === 'delete') {
      const objectIds = ids.filter(i => mongoose.Types.ObjectId.isValid(i)).map(i => mongoose.Types.ObjectId(i));
      const stringSlugs = ids.filter(i => !mongoose.Types.ObjectId.isValid(i));
      const orClauses = [];
      if (objectIds.length) orClauses.push({ _id: { $in: objectIds } });
      if (stringSlugs.length) orClauses.push({ slug: { $in: stringSlugs } });
      if (orClauses.length === 0) return res.status(400).json({ error: 'no_valid_ids' });
      await Article.deleteMany({ $or: orClauses });
      return res.json({ success: true });
    } else if (action === 'publish' || action === 'unpublish') {
      const set = action === 'publish' ? { status: 'published', publishedAt: new Date() } : { status: 'unpublished' };
      const objectIds = ids.filter(i => mongoose.Types.ObjectId.isValid(i)).map(i => mongoose.Types.ObjectId(i));
      const stringSlugs = ids.filter(i => !mongoose.Types.ObjectId.isValid(i));
      const updateFilter = { $or: [] };
      if (objectIds.length) updateFilter.$or.push({ _id: { $in: objectIds } });
      if (stringSlugs.length) updateFilter.$or.push({ slug: { $in: stringSlugs } });
      await Article.updateMany(updateFilter, { $set: set, $currentDate: { updatedAt: true } });
      return res.json({ success: true });
    } else {
      return res.status(400).json({ error: 'unknown_action' });
    }
  } catch (err) {
    console.error('POST /api/admin/news/bulk error:', err);
    return res.status(500).json({ error: 'Bulk action failed', details: err.message });
  }
});



function uploadBufferToCloudinary(buffer, folder = 'uploads') {
  return new Promise((resolve, reject) => {
    const uploadStream = cloudinary.uploader.upload_stream({ folder }, (err, result) => {
      if (err) return reject(err);
      resolve(result);
    });
    streamifier.createReadStream(buffer).pipe(uploadStream);
  });
}

app.post('/api/upload', upload.array('images', 10), async (req, res) => {
  try {
    const files = req.files || [];
    if (!files.length) return res.status(400).json({ success: false, message: 'No files uploaded' });

    const folder = typeof req.body.folder === 'string' && req.body.folder.trim() ? req.body.folder.trim() : 'uploads';
    const uploads = await Promise.all(files.map(f => uploadBufferToCloudinary(f.buffer, folder)));

    const data = uploads.map(u => ({
      url: u.secure_url || u.url,
      public_id: u.public_id,
      width: u.width,
      height: u.height,
      resource_type: u.resource_type,
    }));

    return res.json({ success: true, data });
  } catch (err) {
    console.error('Upload error', err);
    return res.status(500).json({ success: false, message: err.message || 'Upload failed' });
  }
});

// api đơn hàng
const crypto = require('crypto'); // ensure available

const OrderItemSchema = new mongoose.Schema({
  // productId: canonical id of product (tour/bus/flight) when provided by client
  productId: { type: String, default: null, index: true },
  // itemId: legacy field (booking reference / product id depending on client)
  itemId: { type: String, default: null },
  type: { type: String, default: 'unknown' },
  // backend requires name: keep required
  name: { type: String, required: true },
  sku: { type: String, default: null },
  quantity: { type: Number, default: 1, min: 1 },
  unitPrice: { type: Number, default: 0, min: 0 },
  subtotal: { type: Number, default: 0, min: 0 }
}, { _id: false });

const DiscountSchema = new mongoose.Schema({
  code: String,
  name: String,
  amount: { type: Number, default: 0, min: 0 }
}, { _id: false });

const TimelineSchema = new mongoose.Schema({
  ts: { type: Date, default: Date.now },
  text: String,
  meta: { type: mongoose.Schema.Types.Mixed, default: {} }
}, { _id: false });


const TOUR_SERVICE = process.env.TOUR_SERVICE_BASE || 'http://localhost:8080';

function toDateIso(v) {
  try { return (new Date(v)).toISOString().split('T')[0]; } catch { return null; }
}
// New helper: compute seat-consuming pax (adults + children) from snapshot/details.
// Returns { seatCount, adults, children, infants, paxArr }
function seatConsumingCounts(snapshot, it) {
  const paxArr = Array.isArray(snapshot?.details?.passengers) ? snapshot.details.passengers : [];
  let adults = 0, children = 0, infants = 0;
  if (paxArr.length) {
    for (const p of paxArr) {
      const t = (p && p.type) ? String(p.type).toLowerCase() : 'adult';
      if (t === 'infant') infants++;
      else if (t === 'child') children++;
      else adults++;
    }
  } else if (snapshot?.passengers?.counts) {
    const c = snapshot.passengers.counts;
    adults = Number(c.adults || 0);
    children = Number(c.children || 0);
    infants = Number(c.infants || 0);
  } else if (Array.isArray(it?.passengers) && it.passengers.length) {
    // fallback if item itself contains passengers
    for (const p of it.passengers) {
      const t = (p && p.type) ? String(p.type).toLowerCase() : 'adult';
      if (t === 'infant') infants++;
      else if (t === 'child') children++;
      else adults++;
    }
  } else {
    // last fallback: use item.quantity as adults
    const q = Number(it?.quantity || 1);
    adults = Math.max(1, q);
  }
  const seatCount = Math.max(1, adults + children); // infants do NOT consume seats
  return { seatCount, adults, children, infants, paxArr };
}
function paxCountsFromOrder(ord) {
  const snap = ord?.metadata?.bookingDataSnapshot || ord?.metadata || {};
  let adults = 0, children = 0, infants = 0;
  if (Array.isArray(snap?.details?.passengers) && snap.details.passengers.length) {
    for (const p of snap.details.passengers) {
      const t = (p && p.type) ? String(p.type).toLowerCase() : 'adult';
      if (t === 'infant') infants++;
      else if (t === 'child') children++;
      else adults++;
    }
  } else if (snap?.passengers?.counts) {
    const c = snap.passengers.counts;
    adults = Number(c.adults || 0);
    children = Number(c.children || 0);
    infants = Number(c.infants || 0);
  } else if (Array.isArray(ord?.items) && ord.items.length && Array.isArray(ord.items[0]?.passengers) && ord.items[0].passengers.length) {
    // fallback if item itself contains passengers
    for (const p of ord.items[0].passengers) {
      const t = (p && p.type) ? String(p.type).toLowerCase() : 'adult';
      if (t === 'infant') infants++;
      else if (t === 'child') children++;
      else adults++;
    }
  } else {
    // last fallback: use item.quantity as adults
    const q = Number(ord?.items?.[0]?.quantity || 1);
    adults = Math.max(1, q);
  }
  const seatCount = Math.max(1, adults + children); // infants don't consume seats
  return { adults, children, infants, seatCount };
}
// 2 cái re này của tour nhớ
async function reserveViaHttp(tourId, dateIso, paxCount, reservationId = null, orderNumber = null) {
  const body = { tourId, dateIso, paxCount: Number(paxCount || 0) };
  if (reservationId) body.reservationId = reservationId;
  if (orderNumber) body.orderNumber = orderNumber;
  const r = await fetch(`${TOUR_SERVICE}/api/tours/slots/reserve`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body)
  });
  if (!r.ok) {
    const txt = await r.text().catch(() => '');
    throw new Error(`reserve failed ${r.status} ${txt}`);
  }
  return r.json();
}

async function releaseViaHttp(tourId, dateIso, reservationId = null, orderNumber = null) {
  const body = { tourId, dateIso };
  if (reservationId) body.reservationId = reservationId;
  if (orderNumber) body.orderNumber = orderNumber;
  const r = await fetch(`${TOUR_SERVICE}/api/tours/slots/release`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body)
  });
  if (!r.ok) {
    const txt = await r.text().catch(() => '');
    throw new Error(`release failed ${r.status} ${txt}`);
  }
  return r.json();
}

const ORDER_STATUSES_COUNTED = new Set(['pending', 'confirmed', 'processing']);
const PAYMENT_STATUSES_COUNTED = new Set(['pending', 'paid']);

async function tryReserveSlotsForOrder(order) {
  const snapshot = order.metadata?.bookingDataSnapshot;
  if (!snapshot) return [];
  const reservations = [];
  for (const it of order.items || []) {
    if (!it) continue;
    if (it.type !== 'tour') continue;
    const tourId = it.productId || it.itemId;
    if (!tourId) continue;
    const dateRaw = snapshot.details?.startDateTime ?? snapshot.details?.date;
    const dateIso = toDateIso(dateRaw);
    if (!dateIso) continue;
    // determine paxCount
    // let paxCount = 1;
    // if (Array.isArray(snapshot.details?.passengers)) paxCount = snapshot.details.passengers.length;
    // else if (snapshot.passengers?.counts) {
    //   const c = snapshot.passengers.counts;
    //   paxCount = Number(c.adults || 0) + Number(c.children || 0) + Number(c.infants || 0) || 1;
    // } else if (it.quantity) paxCount = Number(it.quantity) || 1;
    // count only adults+children for seat reservation; infants sit with adults and don't consume seat
    const { seatCount } = seatConsumingCounts(snapshot, it);
    const paxCount = seatCount;
    // call reserve endpoint
    await reserveViaHttp(tourId, dateIso, paxCount);
    reservations.push({ tourId, dateIso, paxCount, itemIndexHint: it.itemId || it.productId });
  }
  return reservations;
}

async function tryReleaseReservationsList(resList) {
  if (!Array.isArray(resList)) return;
  for (const r of resList) {
    try {
      // prefer reservationId / orderNumber if present
      const reservationId = r.reservationId || r.orderNumber || null;
      const orderNumber = r.orderNumber || r.reservationId || null;
      if (reservationId || orderNumber) {
        await releaseViaHttp(r.tourId, r.dateIso, reservationId, orderNumber);
      } else {
        // fallback: try release by orderNumber if present in r.tourId metadata shape
        await releaseViaHttp(r.tourId, r.dateIso, null, r.orderNumber || null);
      }
    } catch (e) {
      console.error('release reservation failed', r, e.message);
      // continue - best effort
    }
  }
}
const OrderSchema = new mongoose.Schema({
  orderNumber: { type: String, index: true, unique: true, sparse: true },
  customerId: { type: String, default: null, index: true },
  customerName: { type: String, required: true },
  customerEmail: { type: String, default: '' },
  customerPhone: { type: String, default: '' },
  customerAddress: { type: String, default: '' },
  items: { type: [OrderItemSchema], default: [] },
  hasRefundRequest: { type: Boolean, default: false }, // field này để kiểm tra đơn đã gửi ycau hoàn
  ticketIds: { type: [mongoose.Schema.Types.ObjectId], ref: 'Ticket', default: [] },

  // Change-calendar (ticket re-schedule) support
  // indicates whether this order has requested/applied a date-change
  changeCalendar: { type: Boolean, default: false },
  // new travel date after change (store as YYYY-MM-DD string for consistency with tickets)
  dateChangeCalendar: { type: String, default: null },
  // when changing, keep previous tickets' ids here (initially null)
  oldTicketIDs: { type: [mongoose.Schema.Types.ObjectId], ref: 'Ticket', default: null },

  subtotal: { type: Number, default: 0, min: 0 },
  discounts: { type: [DiscountSchema], default: [] },
  fees: { type: [Object], default: [] },
  tax: { type: Number, default: 0, min: 0 },
  total: { type: Number, default: 0, min: 0 },
  paymentMethod: { type: String, default: 'unknown' },
  paymentStatus: { type: String, enum: ['pending', 'paid', 'failed', 'refunded'], default: 'pending' },
  orderStatus: { type: String, enum: ['pending', 'confirmed', 'processing', 'completed', 'cancelled'], default: 'pending' },
  // gateway-specific transaction ids
  transId: { type: String, default: null },     // MoMo
  zp_trans_id: { type: String, default: null }, // ZaloPay
  paymentReference: { type: String, default: null },
  // Price / payment info related to calendar change (penalty / new price / diff / payment result)
  inforChangeCalendar: {
    penalty: { type: Number, default: 0 },      // phí phạt theo chính sách
    newPrice: { type: Number, default: 0 },     // giá của ngày mới
    diff: { type: Number, default: 0 },         // giá chênh lệch (newPrice - oldPrice +/- penalty)
    paymentType: { type: String, default: 'unknown' }, // 'momo'|'zalopay'|'card'|'cash'|...
    transId: { type: String, default: null },     // MoMo tx for this change
    zp_trans_id: { type: String, default: null }, // ZaloPay tx for this change
    // default to 'pending' until payment for change is completed
    status: { type: String, enum: ['pending', 'paid', 'failed', 'refunded'], default: 'pending' },
    currency: { type: String, default: 'VND' },
    // unique code for change requests (ORD_FORCHANGE_...) — generated at order creation when missing
    codeChange: { type: String, default: null, index: true },
    // additional data payload (store change date and optional meta)
    data: {
      changeDate: { type: String, default: null }, // YYYY-MM-DD of the new date
      note: { type: String, default: '' },
      meta: { type: mongoose.Schema.Types.Mixed, default: {} }
    },
    // Trường mới: Tổng số tiền phải trả cho việc đổi lịch
    totalpayforChange: { type: Number, default: 0 }
  },
  timeline: { type: [TimelineSchema], default: [] },
  notes: { type: [String], default: [] },
  metadata: { type: mongoose.Schema.Types.Mixed, default: {} },
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now }
}, { collection: 'orders' });

// ensure updatedAt
OrderSchema.pre('save', function (next) {
  this.updatedAt = new Date();
  next();
});

// generate orderNumber if missing: ORD-YYYYMMDD-<6HEX>, try multiple times
OrderSchema.pre('validate', async function (next) {
  try {
    if (this.orderNumber && String(this.orderNumber).trim()) return next();
    const date = new Date();
    const datePart = `${date.getFullYear()}${String(date.getMonth() + 1).padStart(2, '0')}${String(date.getDate()).padStart(2, '0')}`;
    const maxTries = 6;
    for (let i = 0; i < maxTries; i++) {
      // numeric 6-digit suffix, pad with leading zeros
      const randNum = String(crypto.randomInt(0, 1000000)).padStart(6, '0'); // e.g. "14898E" -> now numeric like "014898"
      const candidate = `ORD_${datePart}_${randNum}`; // use underscore separators
      const exists = await this.constructor.countDocuments({ orderNumber: candidate }).limit(1);
      if (!exists) {
        this.orderNumber = candidate;
        return next();
      }
    }
    // fallback timestamp-based numeric suffix (use underscore, take last 6 digits)
    this.orderNumber = `ORD_${datePart}_${Date.now().toString().slice(-6)}`;
    return next();
  } catch (err) {
    return next(err);
  }
});
// Ensure a change-code (codeChange) for calendar change is generated at order creation time.
// Will not overwrite if already present.
OrderSchema.pre('validate', function (next) {
  try {
    this.inforChangeCalendar = this.inforChangeCalendar || {};
    if (!this.inforChangeCalendar.codeChange) {
      const date = new Date();
      const datePart = `${date.getFullYear()}${String(date.getMonth() + 1).padStart(2, '0')}${String(date.getDate()).padStart(2, '0')}`;
      // generate 6-digit numeric suffix (similar style to orderNumber)
      const rand = String(Math.floor(Math.random() * 1000000)).padStart(6, '0');
      // use ORD_FORCHANGE_ prefix as requested
      this.inforChangeCalendar.codeChange = `ORD_FORCHANGE_${datePart}_${rand}`;
    }
  } catch (e) {
    // ignore generation errors (will not block order save)
  }
  return next();
});
const Order = mongoose.model('Order', OrderSchema);

// List orders with filters, pagination, sort
app.get('/api/orders', async (req, res) => {
  try {
    const page = Math.max(1, parseInt(req.query.page) || 1);
    const pageSize = Math.max(1, parseInt(req.query.pageSize) || 20);
    const q = req.query.q ? String(req.query.q).trim() : '';
    const paymentStatus = req.query.paymentStatus;
    const orderStatus = req.query.orderStatus;

    const filter = {};
    if (q) {
      const re = new RegExp(q.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i');
      filter.$or = [{ orderNumber: re }, { customerName: re }, { customerEmail: re }, { customerPhone: re }];
    }
    if (paymentStatus) filter.paymentStatus = paymentStatus;
    if (orderStatus) filter.orderStatus = orderStatus;

    const total = await Order.countDocuments(filter);
    const data = await Order.find(filter)
      .sort({ createdAt: -1 })
      .skip((page - 1) * pageSize)
      .limit(pageSize)
      .lean();

    return res.json({ data, pagination: { total, page, pageSize } });
  } catch (err) {
    console.error('GET /api/orders error:', err);
    return res.status(500).json({ error: 'Failed to list orders', details: err.message });
  }
});

// Get single order by id or orderNumber
app.get('/api/orders/:id', async (req, res) => {
  try {
    const id = req.params.id;
    let order = null;
    if (mongoose.Types.ObjectId.isValid(id)) {
      order = await Order.findById(id).lean();
    }
    if (!order) {
      order = await Order.findOne({ orderNumber: id }).lean();
    }
    if (!order) return res.status(404).json({ error: 'Order not found' });
    return res.json(order);
  } catch (err) {
    console.error('GET /api/orders/:id error:', err);
    return res.status(500).json({ error: 'Failed to fetch order', details: err.message });
  }
});
// GET /api/orders/:id/details - Trả về order + tickets api này dành cho khi thanh toán thôi
app.get('/api/orders/:id/details', async (req, res) => {
  try {
    const { id } = req.params;
    let order = null;
    if (mongoose.Types.ObjectId.isValid(id)) order = await Order.findById(id);
    if (!order) order = await Order.findOne({ orderNumber: id });
    if (!order) return res.status(404).json({ error: 'Order not found' });

    // Lấy tickets liên quan, chỉ lấy status 'paid' hoặc 'changed'
    const tickets = await Ticket.find({
      orderId: order._id,
      status: { $in: ['paid', 'changed'] }
    }).sort({ passengerIndex: 1 });
    res.json({ order, tickets });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/orders/:id/details - Mở rộng để trả về oldTickets nếu có,api này dành cho khi xem chi tiết đơn hàng
app.get('/api/orders/:id/client/details', async (req, res) => {
  try {
    const { id } = req.params;
    let order = null;
    if (mongoose.Types.ObjectId.isValid(id)) order = await Order.findById(id);
    if (!order) order = await Order.findOne({ orderNumber: id });
    if (!order) return res.status(404).json({ error: 'Order not found' });

    // Lấy tickets hiện tại (paid hoặc changed)
    const tickets = await Ticket.find({
      orderId: order._id,
      // status: { $in: ['paid', 'changed'] }
    }).sort({ passengerIndex: 1 });

    // Nếu có oldTicketIDs, lấy vé cũ
    let oldTickets = [];
    if (Array.isArray(order.oldTicketIDs) && order.oldTicketIDs.length > 0) {
      oldTickets = await Ticket.find({
        _id: { $in: order.oldTicketIDs }
      }).sort({ passengerIndex: 1 });
    }

    res.json({ order, tickets, oldTickets });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});
// Create order
app.post('/api/orders', async (req, res) => {
  try {
    const payload = req.body || {};
    // temporary default customerId when not provided
    payload.customerId = payload.customerId || '64e65e8d3d5e2b0c8a3e9f12';
    // normalization...
    payload.subtotal = Number(payload.subtotal || 0);
    payload.tax = Number(payload.tax || 0);
    payload.total = Number(payload.total || payload.subtotal - (payload.discount || 0) + (payload.tax || 0));

    // normalize items etc (existing code)...
    if (Array.isArray(payload.items)) {
      payload.items = payload.items.map(it => {
        const item = { ...it };
        item.quantity = Number(item.quantity || 1);
        item.unitPrice = Number(item.unitPrice || 0);
        item.subtotal = Number(item.subtotal || (item.unitPrice * item.quantity));
        if (!item.itemId && item.productId) item.itemId = item.productId;
        if (!item.name) item.name = item.type ? String(item.type) : 'item';
        return item;
      });
    }

    const order = new Order(payload);
    await order.save();

    // // Reserve slots only when orderStatus and paymentStatus are in allowed lists
    // try {
    //   const shouldCount = ORDER_STATUSES_COUNTED.has(order.orderStatus) && PAYMENT_STATUSES_COUNTED.has(order.paymentStatus);
    //   if (shouldCount) {
    //     let reservations = [];
    //     try {
    //       reservations = await tryReserveSlotsForOrder(order);
    //     } catch (err) {
    //       // rollback any partial reservations
    //       console.error('Partial reservation failed, releasing previous reservations:', err.message);
    //       await tryReleaseReservationsList(reservations);
    //       // optionally remove slotReservations metadata
    //       // do not fail order creation; but log and notify
    //     }
    //     if ((reservations || []).length) {
    //       order.metadata = order.metadata || {};
    //       order.metadata.slotReservations = reservations;
    //       await order.save();
    //     }
    //   }
    // } catch (e) {
    //   console.error('post-order reservation step error', e);
    // }

    return res.status(201).json(order);
  } catch (err) {
    console.error('POST /api/orders error:', err);
    return res.status(400).json({ error: 'Failed to create order', details: err.message });
  }
});

app.get('/api/orders/customer/:customerId', async (req, res) => {
  try {
    const customerId = String(req.params.customerId || '').trim();
    if (!customerId) return res.status(400).json({ error: 'customerId required' });

    const page = Math.max(1, parseInt(req.query.page) || 1);
    const pageSize = Math.max(1, parseInt(req.query.pageSize) || 20);
    const paymentStatus = req.query.paymentStatus;
    const orderStatus = req.query.orderStatus;
    const q = req.query.q ? String(req.query.q).trim() : '';

    const filter = { customerId };

    if (paymentStatus) filter.paymentStatus = paymentStatus;
    if (orderStatus) filter.orderStatus = orderStatus;
    if (q) {
      const re = new RegExp(q.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i');
      filter.$or = [{ orderNumber: re }, { customerName: re }, { customerEmail: re }, { customerPhone: re }];
    }

    const total = await Order.countDocuments(filter);
    const data = await Order.find(filter)
      .sort({ createdAt: -1 })
      .skip((page - 1) * pageSize)
      .limit(pageSize)
      .lean();

    return res.json({ data, pagination: { total, page, pageSize } });
  } catch (err) {
    console.error('GET /api/orders/customer/:customerId error:', err);
    return res.status(500).json({ error: 'Failed to fetch orders by customer', details: err.message });
  }
});

// Update order by id or orderNumber
app.put('/api/orders/:id', async (req, res) => {
  try {
    const id = req.params.id;
    const payload = req.body || {};
    // find doc by _id or orderNumber
    let order = null;
    if (mongoose.Types.ObjectId.isValid(id)) {
      order = await Order.findById(id);
    }
    if (!order) order = await Order.findOne({ orderNumber: id });
    if (!order) return res.status(404).json({ error: 'Order not found' });

    // apply updates: allow payment fields to be updated
    const allowed = ['customerName', 'customerEmail', 'customerPhone', 'customerAddress', 'items', 'subtotal', 'discounts', 'fees', 'tax', 'total', 'paymentMethod', 'paymentStatus', 'orderStatus', 'transId', 'zp_trans_id', 'paymentReference', 'metadata', 'notes', 'inforChangeCalendar', 'changeCalendar', 'dateChangeCalendar', 'ticketIds', 'oldTicketIDs', 'hasRefundRequest']; // Thêm các field này
    for (const k of allowed) {
      if (typeof payload[k] !== 'undefined') order[k] = payload[k];
    }
    // push timeline entry if provided
    if (payload.timeline && Array.isArray(payload.timeline)) {
      order.timeline = order.timeline.concat(payload.timeline);
    }
    await order.save();
    return res.json(order);
  } catch (err) {
    console.error('PUT /api/orders/:id error:', err);
    return res.status(400).json({ error: 'Failed to update order', details: err.message });
  }
});

function shouldReserve(order) {
  return String(order.paymentStatus) === 'paid' && String(order.orderStatus) === 'confirmed';
}

// Partial update (patch)
app.patch('/api/orders/:id', async (req, res) => {
  try {
    const id = req.params.id;
    // const existing = await Order.findById(id);
    // safe lookup: try ObjectId -> findById, otherwise lookup by orderNumber
    let existing = null;
    if (mongoose.Types.ObjectId.isValid(id)) existing = await Order.findById(id);
    if (!existing) existing = await Order.findOne({ orderNumber: id });
    if (!existing) return res.status(404).json({ error: 'Order not found' });

    const prevShouldReserve = shouldReserve(existing);

    // apply incoming updates
    Object.keys(req.body || {}).forEach(k => { existing[k] = req.body[k]; });

    const newShouldReserve = shouldReserve(existing);

    // moving from reserved-state -> not reserved-state => release using metadata.slotReservations
    if (prevShouldReserve && !newShouldReserve) {
      try {
        const resList = existing.metadata?.slotReservations;
        if (Array.isArray(resList) && resList.length) {
          await tryReleaseReservationsList(resList);
        }
        if (existing.metadata) existing.metadata.slotReservations = [];
      } catch (e) {
        console.error('release on status change failed', e);
      }
    }

    // moving into reserved-state => reserve (idempotent)
    if (!prevShouldReserve && newShouldReserve) {
      try {
        existing.metadata = existing.metadata || {};
        const existingRes = Array.isArray(existing.metadata.slotReservations) ? existing.metadata.slotReservations : [];

        // compute reservations but skip ones already recorded
        const snapshot = existing.metadata?.bookingDataSnapshot;
        const reservationsToStore = [];

        for (const it of existing.items || []) {
          if (!it || it.type !== 'tour') continue;
          const tourId = it.productId || it.itemId;
          const dateRaw = snapshot?.details?.startDateTime ?? snapshot?.details?.date;
          const dateIso = dateRaw ? toDateIso(dateRaw) : null;
          if (!tourId || !dateIso) continue;

          const already = existingRes.some(r => String(r.tourId) === String(tourId) && String(r.dateIso) === String(dateIso));
          if (already) continue;

          // determine paxCount
          let paxCount = 1;
          if (Array.isArray(snapshot?.details?.passengers)) paxCount = snapshot.details.passengers.length;
          else if (snapshot?.passengers?.counts) {
            const c = snapshot.passengers.counts;
            paxCount = Number(c.adults || 0) + Number(c.children || 0) + Number(c.infants || 0) || 1;
          } else if (it.quantity) paxCount = Number(it.quantity) || 1;

          // call reserve
          try {
            await reserveViaHttp(tourId, dateIso, paxCount);
            const rec = { tourId, dateIso, paxCount, createdAt: new Date().toISOString(), status: 'reserved' };
            reservationsToStore.push(rec);
          } catch (err) {
            console.error('reserve on PATCH failed for', { tourId, dateIso, paxCount }, err.message);
            // on failure, release any partial created in this loop
            if (reservationsToStore.length) {
              await tryReleaseReservationsList(reservationsToStore);
            }
            // do not block patch; return warning
            return res.status(409).json({ success: false, message: 'reserve_failed', detail: err.message });
          }
        } // end for items

        if (reservationsToStore.length) {
          existing.metadata.slotReservations = (existing.metadata.slotReservations || []).concat(reservationsToStore);
        }
      } catch (e) {
        console.error('reserve on status change failed', e);
      }
    }

    await existing.save();
    return res.json({ success: true, data: existing });
  } catch (err) {
    console.error('PATCH /api/orders/:id error', err);
    return res.status(500).json({ error: err.message });
  }
});
// Delete order
app.delete('/api/orders/:id', async (req, res) => {
  try {
    const id = req.params.id;
    let removed = null;
    if (mongoose.Types.ObjectId.isValid(id)) {
      removed = await Order.findByIdAndDelete(id).lean();
    }
    if (!removed) {
      removed = await Order.findOneAndDelete({ orderNumber: id }).lean();
    }
    if (!removed) return res.status(404).json({ error: 'Order not found' });
    return res.json({ success: true, id: removed._id || removed.orderNumber });
  } catch (err) {
    console.error('DELETE /api/orders/:id error:', err);
    return res.status(500).json({ error: 'Failed to delete order', details: err.message });
  }
});

// helper route: mark paid (for internal use)
app.post('/api/orders/:id/mark-paid', async (req, res) => {
  try {
    const id = req.params.id;
    const { method = 'unknown', txnId = null } = req.body || {};
    let order = null;
    if (mongoose.Types.ObjectId.isValid(id)) order = await Order.findById(id);
    if (!order) order = await Order.findOne({ orderNumber: id });
    if (!order) return res.status(404).json({ error: 'Order not found' });

    order.paymentStatus = 'paid';
    order.orderStatus = 'confirmed';
    if (method === 'momo') order.transId = txnId || order.transId;
    else if (method === 'zalopay') order.zp_trans_id = txnId || order.zp_trans_id;
    else order.paymentReference = txnId || order.paymentReference;

    order.timeline.push({ text: `Marked paid via ${method}`, meta: { txnId } });
    await order.save();
    return res.json({ ok: true, order });
  } catch (err) {
    console.error('POST /api/orders/:id/mark-paid error:', err);
    return res.status(500).json({ error: 'Failed to mark paid', details: err.message });
  }
});

// New endpoint: Handle change calendar request and update inforChangeCalendar
app.post('/api/orders/:id/change-calendar', async (req, res) => {
  try {
    const { id } = req.params;
    const { newDate, newTime, selectedOption, passengers, changeFeePerPax, fareDiff, totalpayforChange, selectedSeats } = req.body;

    // Validate input cơ bản
    if (!newDate || !selectedOption) {
      return res.status(400).json({ error: 'Missing required fields: newDate, selectedOption' });
    }

    // Find order
    let order = null;
    if (mongoose.Types.ObjectId.isValid(id)) {
      order = await Order.findById(id);
    }
    if (!order) order = await Order.findOne({ orderNumber: id });
    if (!order) return res.status(404).json({ error: 'Order not found' });

    // Extract snapshot and item
    const snapshot = order.metadata?.bookingDataSnapshot || order.metadata || {};
    const item = Array.isArray(order.items) && order.items[0] ? order.items[0] : null;
    if (!item) return res.status(400).json({ error: 'Order has no items' });

    // Calculate pax counts
    const pc = paxCountsFromOrder(order);
    console.log('paxCounts:', pc); // Debug log
    const adults = pc.adults;
    const children = pc.children;
    const infants = pc.infants;
    const totalPax = adults + children; // Giả sử không tính infant cho phạt bus

    // Validate pax counts
    if (totalPax <= 0) {
      return res.status(400).json({ error: 'Invalid passenger counts' });
    }

    // Calculate new total base
    let computedNewTotalBase = 0;
    if (selectedOption.perPax) {
      const pp = selectedOption.perPax;
      computedNewTotalBase = (Number(pp.adult || 0) * adults) + (Number(pp.child || 0) * children) + (Number(pp.infant || 0) * infants);
    } else {
      computedNewTotalBase = Number(selectedOption.fare || 0);
    }
    console.log('computedNewTotalBase:', computedNewTotalBase); // Debug log

    // Original totals
    const origBase = Number(order?.metadata?.bookingDataSnapshot?.pricing?.basePrice ?? order?.subtotal ?? (order?.total ? (Number(order.total) - Number(order.tax || 0)) : 0));
    const origTax = Number(order?.metadata?.bookingDataSnapshot?.pricing?.taxes ?? order?.tax ?? 0);
    const computedNewTax = origBase > 0 ? Math.round(origTax * (computedNewTotalBase / origBase)) : origTax;
    const computedNewTotal = computedNewTotalBase + computedNewTax;
    const currentTotal = Number(order.total || 0);
    const diff = computedNewTotal - currentTotal;
    console.log('diff:', diff, 'currentTotal:', currentTotal, 'computedNewTotal:', computedNewTotal); // Debug log

    // Penalty calculation for bus
    let penAmount = 0;
    if (item.type === 'bus') {
      const sdRaw = snapshot?.details?.startDateTime ?? snapshot?.details?.date ?? order.createdAt;
      if (sdRaw) {
        const sd = new Date(sdRaw);
        const now = new Date();
        const hoursDiff = (sd.getTime() - now.getTime()) / (1000 * 60 * 60);
        console.log('hoursDiff:', hoursDiff); // Debug log
        if (hoursDiff >= 72) {
          penAmount = 50000 * totalPax;
        } else if (hoursDiff >= 24) {
          penAmount = 50000 * totalPax + 0.25 * currentTotal;
        } else {
          return res.status(400).json({ error: 'Cannot change calendar: less than 24 hours before departure' });
        }
      }
    } else {
      // Tour/flight: giữ nguyên logic cũ
      let daysUntilService = Infinity;
      try {
        const sdRaw = snapshot?.details?.startDateTime ?? snapshot?.details?.date ?? order.createdAt;
        if (sdRaw) {
          const sd = new Date(sdRaw);
          const today = new Date();
          const t0 = Date.UTC(today.getFullYear(), today.getMonth(), today.getDate());
          const t1 = Date.UTC(sd.getFullYear(), sd.getMonth(), sd.getDate());
          daysUntilService = Math.ceil((t1 - t0) / (1000 * 60 * 60 * 24));
        }
      } catch (e) {
        console.warn('Error calculating days until service:', e.message);
      }
      let pp = 0;
      if (typeof daysUntilService === 'number') {
        if (daysUntilService > 5) pp = 0.30;
        else if (daysUntilService > 3) pp = 0.50;
        else pp = 1.00;
      }
      penAmount = Math.round(Number(currentTotal) * pp);
    }
    console.log('penAmount:', penAmount); // Debug log

    // Amount due/refund
    let amountDue = 0;
    let refund = 0;
    if (diff >= 0) {
      amountDue = Math.max(0, diff) + penAmount;
    } else {
      const refundGross = Math.max(0, -diff);
      if (refundGross > penAmount) {
        refund = refundGross - penAmount;
        amountDue = 0;
      } else {
        refund = 0;
        amountDue = penAmount - refundGross;
      }
    }
    console.log('amountDue:', amountDue, 'refund:', refund); // Debug log

    // Validate amountDue không NaN
    if (isNaN(amountDue)) {
      return res.status(400).json({ error: 'Invalid amountDue calculation' });
    }

    // Generate codeChange
    const codeChange = `ORD_FORCHANGE_${Date.now()}_${Math.floor(Math.random() * 1000)}`;

    // Update order
    order.inforChangeCalendar = {
      penalty: penAmount,
      newPrice: computedNewTotal,
      diff: diff,
      paymentType: amountDue > 0 ? 'pay' : 'refund',
      transId: null,
      zp_trans_id: null,
      status: 'pending',
      currency: 'VND',
      codeChange: codeChange,
      data: {
        changeDate: newDate,
        note: `Đổi lịch sang ${newDate} ${newTime}`,
        meta: {
          newTime,
          selectedOption,
          passengers,
          changeFeePerPax,
          fareDiff,
          selectedSeats: Array.isArray(selectedSeats) ? selectedSeats : [] // Lưu selectedSeats
        }
      },
      totalpayforChange: totalpayforChange
    };
    order.changeCalendar = true;
    order.dateChangeCalendar = newDate;

    await order.save();

    // Response
    res.json({
      codeChange,
      amountDue,
      refund,
      penaltyAmount: penAmount,
      newTotal: computedNewTotal,
      diff
    });
  } catch (err) {
    console.error('Error updating change calendar:', err);
    res.status(500).json({ error: 'Internal server error', details: err.message });
  }
});



// api hỗ trợ khách hàng

/*
 Support Tickets API
 - categories (type): technical, billing, account, general, complaint
 - statuses: new, open, pending, resolved, closed
 - optional serviceType: flight | bus | tour
*/

const SUPPORT_TYPES = {
  technical: 'Kỹ thuật',
  billing: 'Thanh toán',
  account: 'Tài khoản',
  general: 'Chung',
  complaint: 'Khiếu nại',
  cancel: 'Hủy đơn',
};

const SUPPORT_STATUS_LABELS = {
  new: 'Mới',
  open: 'Đang xử lý',
  pending: 'Chờ phản hồi',
  resolved: 'Đã giải quyết',
  closed: 'Đã đóng',
};

const SupportMessageSchema = new mongoose.Schema({
  authorType: { type: String, enum: ['customer', 'agent'], required: true },
  authorId: { type: String, default: null },
  text: { type: String, required: true },
  attachments: { type: [mongoose.Schema.Types.Mixed], default: [] },
  createdAt: { type: Date, default: Date.now }
}, { _id: false });

const SupportTicketSchema = new mongoose.Schema({
  ticketNumber: { type: String, index: true, unique: true, sparse: true },
  customerId: { type: String, index: true, default: null },

  // store basic customer contact snapshot on ticket for easier processing
  customerName: { type: String, default: null },
  customerEmail: { type: String, default: null },
  customerPhone: { type: String, default: null },

  serviceType: { type: String, enum: ['flight', 'bus', 'tour'], default: null },
  type: { type: String, enum: Object.keys(SUPPORT_TYPES), required: true },
  title: { type: String, required: true },
  description: { type: String, default: '' },

  // refundInfo groups payment/refund/order reference details
  refundInfo: {
    orderRef: { type: String, default: null },
    transId: { type: String, default: null },
    zp_trans_id: { type: String, default: null },
    paymentReference: { type: String, default: null },
    airlinePenalty: { type: Number, default: 0 },
    taxes: { type: Number, default: 0 },
    platformFee: { type: Number, default: 0 },
    refundAmount: { type: Number, default: 0 },
    currency: { type: String, default: 'VND' }
  },

  status: { type: String, enum: Object.keys(SUPPORT_STATUS_LABELS), default: 'new' },
  assignee: { id: String, name: String, role: String },
  messages: { type: [SupportMessageSchema], default: [] },
  metadata: { type: mongoose.Schema.Types.Mixed, default: {} },
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now }
}, { collection: 'support_tickets' });

SupportTicketSchema.pre('save', function (next) { this.updatedAt = new Date(); next(); });

// generate ticketNumber if missing
SupportTicketSchema.pre('validate', async function (next) {
  try {
    if (this.ticketNumber && String(this.ticketNumber).trim()) return next();
    const date = new Date();
    const datePart = `${date.getFullYear()}${String(date.getMonth() + 1).padStart(2, '0')}${String(date.getDate()).padStart(2, '0')}`;
    const rand = String(Math.floor(Math.random() * 900000) + 100000); // 6 digits
    this.ticketNumber = `TICKET_${datePart}_${rand}`;
    return next();
  } catch (e) { return next(e); }
});

const SupportTicket = mongoose.model('SupportTicket', SupportTicketSchema);

// Create ticket
app.post('/api/support', async (req, res) => {
  try {
    const body = req.body || {};

    // accept refund/payment snapshot fields and customer contact info from frontend
    const refundInfoPayload = {
      orderRef: body.orderRef || body.orderNumber || null,
      transId: body.transId || null,
      zp_trans_id: body.zp_trans_id || null,
      paymentReference: body.paymentReference || null,
      airlinePenalty: Number(body.airlinePenalty || 0),
      taxes: Number(body.taxes || 0),
      platformFee: Number(body.platformFee || 0),
      refundAmount: Number(body.refundAmount || 0),
      currency: body.currency || 'VND'
    };

    const payload = {
      customerId: body.customerId || body.customer || null,
      customerName: body.customerName || null,
      customerEmail: body.customerEmail || null,
      customerPhone: body.customerPhone || null,
      serviceType: (['flight', 'bus', 'tour'].includes(body.serviceType) ? body.serviceType : null),
      type: Object.keys(SUPPORT_TYPES).includes(body.type) ? body.type : 'general',
      title: String(body.title || '').trim(),
      description: String(body.description || '').trim(),

      refundInfo: refundInfoPayload,
      messages: [],
      metadata: body.metadata || {}
    };
    if (!payload.title) return res.status(400).json({ error: 'title_required' });

    // initial message optional
    if (body.message && String(body.message).trim()) {
      payload.messages.push({
        authorType: 'customer',
        authorId: payload.customerId,
        text: String(body.message).trim(),
        attachments: Array.isArray(body.attachments) ? body.attachments : []
      });
    }

    const ticket = new SupportTicket(payload);
    await ticket.save();

    // Nếu type 'cancel', update order.hasRefundRequest = true
    if (payload.type === 'cancel' && payload.refundInfo?.orderRef) {
      try {
        let order = null;
        if (mongoose.Types.ObjectId.isValid(payload.refundInfo.orderRef)) {
          order = await Order.findById(payload.refundInfo.orderRef);
        }
        if (!order) order = await Order.findOne({ orderNumber: payload.refundInfo.orderRef });
        if (order) {
          order.hasRefundRequest = true;
          await order.save();
        }
      } catch (e) {
        console.warn('Failed to update order.hasRefundRequest', e.message);
      }
    }

    return res.status(201).json({ ok: true, ticket });
  } catch (err) {
    console.error('POST /api/support error', err);
    return res.status(500).json({ ok: false, error: 'create_failed', details: err.message });
  }
});

// List tickets with filters (customer, status, type, serviceType) + pagination
app.get('/api/support', async (req, res) => {
  try {
    const page = Math.max(1, parseInt(req.query.page) || 1);
    const pageSize = Math.max(1, parseInt(req.query.pageSize) || 20);
    const customerId = req.query.customerId ? String(req.query.customerId).trim() : null;
    const status = req.query.status ? String(req.query.status).trim() : null;
    const type = req.query.type ? String(req.query.type).trim() : null;
    const serviceType = req.query.serviceType ? String(req.query.serviceType).trim() : null;
    const q = req.query.q ? String(req.query.q).trim() : null;

    const filter = {};
    if (customerId) filter.customerId = customerId;
    if (status && Object.keys(SUPPORT_STATUS_LABELS).includes(status)) filter.status = status;
    if (type && Object.keys(SUPPORT_TYPES).includes(type)) filter.type = type;
    if (serviceType && ['flight', 'bus', 'tour'].includes(serviceType)) filter.serviceType = serviceType;
    if (q) {
      const re = new RegExp(q.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i');
      filter.$or = [{ title: re }, { description: re }, { ticketNumber: re }, { orderRef: re }];
    }

    const total = await SupportTicket.countDocuments(filter);
    const data = await SupportTicket.find(filter)
      .sort({ updatedAt: -1, createdAt: -1 })
      .skip((page - 1) * pageSize)
      .limit(pageSize)
      .lean();

    // add human labels for convenience
    const mapped = data.map(d => ({
      ...d,
      typeLabel: SUPPORT_TYPES[d.type] || d.type,
      statusLabel: SUPPORT_STATUS_LABELS[d.status] || d.status,
    }));

    return res.json({ ok: true, data: mapped, pagination: { total, page, pageSize } });
  } catch (err) {
    console.error('GET /api/support error', err);
    return res.status(500).json({ ok: false, error: 'list_failed', details: err.message });
  }
});

// Get single ticket
app.get('/api/support/:id', async (req, res) => {
  try {
    const id = req.params.id;
    let ticket = null;
    if (mongoose.Types.ObjectId.isValid(id)) ticket = await SupportTicket.findById(id).lean();
    if (!ticket) ticket = await SupportTicket.findOne({ ticketNumber: id }).lean();
    if (!ticket) return res.status(404).json({ ok: false, error: 'not_found' });
    ticket.typeLabel = SUPPORT_TYPES[ticket.type] || ticket.type;
    ticket.statusLabel = SUPPORT_STATUS_LABELS[ticket.status] || ticket.status;
    return res.json({ ok: true, ticket });
  } catch (err) {
    console.error('GET /api/support/:id error', err);
    return res.status(500).json({ ok: false, error: 'fetch_failed', details: err.message });
  }
});

// Update ticket (status, assignee, title, description, metadata)
app.put('/api/support/:id', async (req, res) => {
  try {
    const id = req.params.id;
    const body = req.body || {};
    let ticket = null;
    if (mongoose.Types.ObjectId.isValid(id)) ticket = await SupportTicket.findById(id);
    if (!ticket) ticket = await SupportTicket.findOne({ ticketNumber: id });
    if (!ticket) return res.status(404).json({ ok: false, error: 'not_found' });

    if (typeof body.status !== 'undefined' && Object.keys(SUPPORT_STATUS_LABELS).includes(body.status)) ticket.status = body.status;
    if (typeof body.assignee !== 'undefined') ticket.assignee = body.assignee;
    if (typeof body.title !== 'undefined') ticket.title = String(body.title).trim();
    if (typeof body.description !== 'undefined') ticket.description = String(body.description).trim();
    if (typeof body.metadata !== 'undefined') ticket.metadata = Object.assign({}, ticket.metadata || {}, body.metadata);

    await ticket.save();

    // Post-process: if this is a cancel ticket and now resolved -> release tour slots  update order to refunded/cancelled
    (async () => {
      try {
        if (ticket.type === 'cancel' && ticket.status === 'resolved') {
          const refundInfo = ticket.refundInfo || ticket.metadata?.refundInfo || {};
          const orderRef = refundInfo.orderRef || ticket.metadata?.orderRef || null;
          if (orderRef) {
            // locate order by _id or orderNumber
            let order = null;
            if (mongoose.Types.ObjectId.isValid(orderRef)) order = await Order.findById(orderRef);
            if (!order) order = await Order.findOne({ orderNumber: orderRef });

            if (order) {
              // release tour slots for tour items (best-effort)
              const snapshot = order.metadata?.bookingDataSnapshot || {};
              for (const it of order.items || []) {
                try {
                  if (!it || it.type !== 'tour') continue;
                  const tourId = it.productId || it.itemId;
                  if (!tourId) continue;
                  const dateRaw = snapshot.details?.startDateTime ?? snapshot.details?.date ?? order.createdAt;
                  const dateIso = toDateIso(dateRaw);
                  if (!dateIso) continue;
                  // paxCount derivation
                  let paxCount = 1;
                  if (Array.isArray(snapshot.details?.passengers)) paxCount = snapshot.details.passengers.length;
                  else if (snapshot.passengers?.counts) {
                    const c = snapshot.passengers.counts;
                    paxCount = Number(c.adults || 0) + Number(c.children || 0) + Number(c.infants || 0) || 1;
                  } else if (it.quantity) paxCount = Number(it.quantity) || 1;

                  // // call tour-service release endpoint (reuse helper releaseViaHttp)
                  // await releaseViaHttp(tourId, dateIso, paxCount);
                  // call tour-service release endpoint (use orderNumber/reservationId for idempotent cancel)
                  const reservationId = order.orderNumber || refundInfo.orderRef || String(order._id);
                  await releaseViaHttp(tourId, dateIso, reservationId, reservationId);
                } catch (e) {
                  console.error('release slot failed for order item', it, e && e.message ? e.message : e);
                  // continue other items
                }
              }

              // release bus slots for bus items (best-effort)
              const SELF_BASE = process.env.SELF_BASE || `http://localhost:${port || 7700}`;
              for (const it of order.items || []) {
                try {
                  if (!it || it.type !== 'bus') continue;
                  const busId = it.productId || it.itemId;
                  if (!busId) continue;
                  // date from snapshot.meta.departureDateIso (preferred) or details.date
                  const dateRawBus = snapshot.meta?.departureDateIso ?? snapshot.details?.date ?? order.createdAt;
                  const dateIsoBus = toDateIso(dateRawBus);
                  if (!dateIsoBus) continue;

                  const seats = Array.isArray(snapshot.details?.seats) && snapshot.details.seats.length ? snapshot.details.seats : undefined;
                  const paxCount = seats ? undefined : (Array.isArray(snapshot.details?.passengers) ? snapshot.details.passengers.length : (Number(it.quantity || 1) || 1));
                  const reservationId = order.orderNumber || refundInfo.orderRef || null;

                  const body = { dateIso: dateIsoBus };
                  if (seats) body.seats = seats;
                  else body.count = paxCount;
                  if (reservationId) body.reservationId = reservationId;

                  await axios.post(`${SELF_BASE}/api/buses/${encodeURIComponent(busId)}/slots/release`, body, { timeout: 5000 });
                  console.log('Released bus slots', { busId, dateIso: dateIsoBus, reservationId, seats, paxCount });
                } catch (e) {
                  console.error('release bus slot failed for order item', it, e && e.message ? e.message : e);
                  // continue - best effort
                }
              }

              // update status ticket
              // Cancel related tickets for this order (idempotent) by calling local tickets API
              try {
                const ticketIds = Array.isArray(order.ticketIds) ? order.ticketIds : [];
                if (ticketIds.length) {
                  for (const tid of ticketIds) {
                    try {
                      // call ticket status endpoint on this service
                      await axios.patch(`${SELF_BASE}/api/tickets/${encodeURIComponent(tid)}/status`, {
                        status: 'cancelled',
                        reason: 'support_reservation_released',
                        by: { supportTicket: ticket.ticketNumber }
                      }, { timeout: 8000 }).catch(() => { });
                    } catch (e) {
                      console.warn('cancel ticket failed for', tid, e && e.message ? e.message : e);
                    }
                  }
                  console.log('Cancelled tickets for order', order.orderNumber || order._id, 'count=', ticketIds.length);
                } else {
                  console.log('No ticketIds found on order to cancel for', order.orderNumber || order._id);
                }
              } catch (e) {
                console.error('cancelTicketsForOrder HTTP loop failed', e && e.message ? e.message : e);
              }


              // update order status/paymentStatus and append timeline/metadata
              try {
                order.paymentStatus = 'refunded';
                order.orderStatus = 'cancelled';
                order.timeline = order.timeline || [];
                order.timeline.push({ ts: new Date(), text: `Cancelled/refunded via support ticket ${ticket.ticketNumber}`, meta: { ticket: ticket.ticketNumber } });
                order.metadata = order.metadata || {};
                order.metadata.refundProcessedBy = ticket.ticketNumber;
                order.metadata.refundInfo = Object.assign({}, order.metadata.refundInfo || {}, refundInfo);
                await order.save();
                console.log(`Order ${order.orderNumber || order._id} marked refunded/cancelled by support ${ticket.ticketNumber}`);
              } catch (e) {
                console.error('Failed updating order status after support resolve', e);
              }
            } else {
              console.warn('Support resolve: related order not found', orderRef);
            }
          } else {
            console.warn('Support resolve: no orderRef available on ticket', ticket.ticketNumber);
          }
        }
      } catch (e) {
        console.error('support cancel post-process error', e);
      }
    })();

    return res.json({ ok: true, ticket });
  } catch (err) {
    console.error('PUT /api/support/:id error', err);
    return res.status(500).json({ ok: false, error: 'update_failed', details: err.message });
  }
});

// Add message to ticket
app.post('/api/support/:id/messages', async (req, res) => {
  try {
    const id = req.params.id;
    const body = req.body || {};
    if (!body.text || !String(body.text).trim()) return res.status(400).json({ ok: false, error: 'text_required' });

    let ticket = null;
    if (mongoose.Types.ObjectId.isValid(id)) ticket = await SupportTicket.findById(id);
    if (!ticket) ticket = await SupportTicket.findOne({ ticketNumber: id });
    if (!ticket) return res.status(404).json({ ok: false, error: 'not_found' });

    const msg = {
      authorType: (body.authorType === 'agent' ? 'agent' : 'customer'),
      authorId: body.authorId || null,
      text: String(body.text).trim(),
      attachments: Array.isArray(body.attachments) ? body.attachments : []
    };

    ticket.messages = ticket.messages || [];
    ticket.messages.push(msg);

    // update status heuristics
    if (msg.authorType === 'customer') {
      // customer reply -> set to pending (waiting agent) unless already new/open
      ticket.status = ticket.status === 'new' ? 'open' : 'pending';
    } else {
      // agent reply -> reopen if closed/resolved
      if (ticket.status === 'new' || ticket.status === 'pending') ticket.status = 'open';
    }

    await ticket.save();
    return res.json({ ok: true, message: msg, ticket });
  } catch (err) {
    console.error('POST /api/support/:id/messages error', err);
    return res.status(500).json({ ok: false, error: 'message_failed', details: err.message });
  }
});

// simple admin bulk close
app.post('/api/support/bulk', async (req, res) => {
  try {
    const { action, ids } = req.body || {};
    if (!Array.isArray(ids) || ids.length === 0) return res.status(400).json({ ok: false, error: 'ids_required' });

    const objectIds = ids.filter(i => mongoose.Types.ObjectId.isValid(i)).map(i => mongoose.Types.ObjectId(i));
    const ticketNumbers = ids.filter(i => !mongoose.Types.ObjectId.isValid(i)).map(i => String(i));

    const or = [];
    if (objectIds.length) or.push({ _id: { $in: objectIds } });
    if (ticketNumbers.length) or.push({ ticketNumber: { $in: ticketNumbers } });
    if (or.length === 0) return res.status(400).json({ ok: false, error: 'no_valid_ids' });

    if (action === 'close') {
      await SupportTicket.updateMany({ $or: or }, { $set: { status: 'closed', updatedAt: new Date() } });
      return res.json({ ok: true });
    } else if (action === 'resolve') {
      await SupportTicket.updateMany({ $or: or }, { $set: { status: 'resolved', updatedAt: new Date() } });
      return res.json({ ok: true });
    } else {
      return res.status(400).json({ ok: false, error: 'unknown_action' });
    }
  } catch (err) {
    console.error('POST /api/support/bulk error', err);
    return res.status(500).json({ ok: false, error: 'bulk_failed', details: err.message });
  }
});
app.delete('/api/support/:id', async (req, res) => {
  try {
    const id = req.params.id;
    let removed = null;

    // try delete by Mongo _id first
    if (mongoose.Types.ObjectId.isValid(id)) {
      removed = await SupportTicket.findByIdAndDelete(id).lean();
    }

    // fallback: delete by ticketNumber
    if (!removed) {
      removed = await SupportTicket.findOneAndDelete({ ticketNumber: id }).lean();
    }

    if (!removed) {
      return res.status(404).json({ ok: false, error: 'not_found' });
    }

    return res.json({ ok: true, id: removed._id || removed.ticketNumber || id });
  } catch (err) {
    console.error('DELETE /api/support/:id error', err);
    return res.status(500).json({ ok: false, error: 'delete_failed', details: err.message });
  }
});





//TICKET

// ...existing code...
/* ----------------- Ticket: generic model + CRUD API ----------------- */
const TicketSchema = new mongoose.Schema({
  ticketNumber: { type: String, required: true, index: true, unique: true },
  orderId: { type: mongoose.Schema.Types.ObjectId, ref: 'Order', index: true, default: null },
  orderNumber: { type: String, index: true, default: null },

  // 'bus' | 'tour' | 'flight' | 'other'
  type: { type: String, required: true, index: true, default: 'other' },

  productId: { type: String, index: true, default: null }, // busId / tourId / flightId
  providerReservationId: { type: String, default: null, index: true },

  passengerIndex: { type: Number, default: null },
  passenger: {
    name: String,
    type: String,
    idNumber: String,
    dob: String

  },

  seats: { type: [String], default: [] }, // seat ids for bus/flight
  travelDate: { type: String, default: null }, // YYYY-MM-DD
  travelStart: { type: Date, default: null },
  travelEnd: { type: Date, default: null },

  price: { type: Number, default: 0 },
  currency: { type: String, default: 'VND' },

  reservationInfo: { type: mongoose.Schema.Types.Mixed, default: {} },

  // New: ticketType per passenger
  ticketType: {
    type: String,
    enum: ['adult', 'child', 'infant'],
    required: [true, 'Loại vé là bắt buộc'],
    default: 'adult'
  },

  // Reduce status enum to four states
  status: { type: String, enum: ['paid', 'cancelled', 'changed'], default: 'paid', index: true },

  statusHistory: {
    type: [new mongoose.Schema({
      from: String, to: String, by: mongoose.Schema.Types.Mixed, reason: String, meta: mongoose.Schema.Types.Mixed, ts: { type: Date, default: Date.now }
    }, { _id: false })], default: []
  },

  uniq: { type: String, index: true, sparse: true }, // idempotency key

  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now }
}, { collection: 'tickets' });

TicketSchema.pre('save', function (next) { this.updatedAt = new Date(); next(); });

const Ticket = mongoose.model('Ticket', TicketSchema);

// helper: safe string
function safeStr(v) { return v == null ? '' : String(v); }

// Create ticket (idempotent if uniq provided)
app.post('/api/tickets', async (req, res) => {
  try {
    const p = req.body || {};
    // Build uniq if not provided
    const uniq = p.uniq || `${safeStr(p.orderNumber)}::${safeStr(p.type)}::${safeStr(p.productId)}::${safeStr(p.travelDate || p.travelStart)}::${p.passengerIndex != null ? 'paxIndex:' + String(p.passengerIndex) : (Array.isArray(p.seats) && p.seats.length ? 'seats:' + p.seats.join(',') : '')}`;
    if (uniq) {
      const exists = await Ticket.findOne({ uniq }).lean();
      if (exists) return res.status(200).json({ ok: true, ticket: exists, note: 'exists' });
    }

    const ticketNumber = p.ticketNumber || `TKT_${(p.orderNumber || 'ORD').slice(0, 20)}_${String(Math.floor(Math.random() * 900000) + 100000)}`;

    // Normalize passenger according to schema: if schema expects String, stringify object
    let passengerVal = p.passenger || {};
    try {
      const passengerPath = Ticket.schema && Ticket.schema.path && Ticket.schema.path('passenger');
      if (passengerPath && passengerPath.instance === 'String' && passengerVal && typeof passengerVal === 'object') {
        passengerVal = JSON.stringify(passengerVal);
      }
    } catch (e) {
      // ignore - fallback to original object
    }

    // determine ticketType robustly (from explicit p.ticketType, or passenger.type if available)
    let ticketTypeVal = p.ticketType || null;
    if (!ticketTypeVal) {
      if (passengerVal && typeof passengerVal === 'object') ticketTypeVal = passengerVal.type || passengerVal.ticketType || null;
      else if (typeof passengerVal === 'string') {
        try {
          const parsed = JSON.parse(passengerVal);
          ticketTypeVal = parsed && (parsed.type || parsed.ticketType) ? (parsed.type || parsed.ticketType) : null;
        } catch (e) { /* ignore */ }
      }
    }
    if (!ticketTypeVal) ticketTypeVal = 'adult';

    const t = new Ticket({
      ticketNumber,
      orderId: p.orderId ? (mongoose.Types.ObjectId.isValid(p.orderId) ? new mongoose.Types.ObjectId(p.orderId) : null) : null,
      orderNumber: p.orderNumber || null,
      type: p.type || 'other',
      productId: p.productId || null,
      providerReservationId: p.providerReservationId || null,
      passengerIndex: typeof p.passengerIndex === 'number' ? p.passengerIndex : (p.passengerIndex ? Number(p.passengerIndex) : null),
      passenger: passengerVal,
      seats: Array.isArray(p.seats) ? p.seats : (p.seat ? [p.seat] : []),
      travelDate: p.travelDate || (p.travelStart ? toDateIso(p.travelStart) : null),
      travelStart: p.travelStart ? new Date(p.travelStart) : null,
      travelEnd: p.travelEnd ? new Date(p.travelEnd) : null,
      price: Number(p.price || 0),
      currency: p.currency || 'VND',
      reservationInfo: p.reservationInfo || {},
      status: p.status || 'paid',
      ticketType: ticketTypeVal,
      uniq: uniq || null
    });
    await t.save();
    // push ticket _id into order.ticketIds when possible (idempotent)
    try {
      if (t.orderId) {
        const ord = await Order.findById(t.orderId);
        if (ord) {
          ord.ticketIds = Array.isArray(ord.ticketIds) ? ord.ticketIds : [];
          if (!ord.ticketIds.some(id => String(id) === String(t._id))) {
            ord.ticketIds.push(t._id);
            await ord.save();
          }
        }
      } else if (t.orderNumber) {
        const ord = await Order.findOne({ orderNumber: t.orderNumber });
        if (ord) {
          ord.ticketIds = Array.isArray(ord.ticketIds) ? ord.ticketIds : [];
          if (!ord.ticketIds.some(id => String(id) === String(t._id))) {
            ord.ticketIds.push(t._id);
            await ord.save();
          }
        }
      }
    } catch (e) {
      // non-fatal
      console.warn('ticket->order link save failed', e && e.message ? e.message : e);
    }

    return res.status(201).json({ ok: true, ticket: t });
  } catch (err) {
    console.error('POST /api/tickets error', err && err.message ? err.message : err);
    return res.status(500).json({ ok: false, error: 'create_failed', details: err.message });
  }
});

// List tickets: support filtering by orderNumber, type, productId, status
app.get('/api/tickets', async (req, res) => {
  try {
    const page = Math.max(1, parseInt(req.query.page) || 1);
    const pageSize = Math.max(1, parseInt(req.query.pageSize) || 50);
    const q = req.query.q ? String(req.query.q).trim() : '';
    const filter = {};
    if (q) {
      const re = new RegExp(q.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i');
      filter.$or = [{ ticketNumber: re }, { orderNumber: re }, { 'passenger.name': re }, { productId: re }];
    }
    if (req.query.type) filter.type = req.query.type;
    if (req.query.productId) filter.productId = req.query.productId;
    if (req.query.status) filter.status = req.query.status;
    const total = await Ticket.countDocuments(filter);
    const data = await Ticket.find(filter).sort({ createdAt: -1 }).skip((page - 1) * pageSize).limit(pageSize).lean();
    return res.json({ ok: true, total, page, pageSize, data });
  } catch (err) {
    console.error('GET /api/tickets error', err);
    return res.status(500).json({ ok: false, error: 'list_failed', details: err.message });
  }
});

// Get single ticket by id or ticketNumber
app.get('/api/tickets/:id', async (req, res) => {
  try {
    const id = req.params.id;
    let t = null;
    if (mongoose.Types.ObjectId.isValid(id)) t = await Ticket.findById(id).lean();
    if (!t) t = await Ticket.findOne({ ticketNumber: id }).lean();
    if (!t) return res.status(404).json({ ok: false, error: 'not_found' });
    return res.json({ ok: true, ticket: t });
  } catch (err) {
    console.error('GET /api/tickets/:id error', err);
    return res.status(500).json({ ok: false, error: 'fetch_failed', details: err.message });
  }
});

// Update ticket (replace allowed fields)
app.put('/api/tickets/:id', async (req, res) => {
  try {
    const id = req.params.id;
    const payload = req.body || {};
    const update = {};
    const allowed = ['status', 'passenger', 'seats', 'price', 'currency', 'reservationInfo', 'providerReservationId', 'travelDate', 'travelStart', 'travelEnd', 'productId', 'type', 'orderNumber'];
    for (const k of allowed) if (typeof payload[k] !== 'undefined') update[k] = payload[k];
    update.updatedAt = new Date();
    const t = await Ticket.findOneAndUpdate(mongoose.Types.ObjectId.isValid(id) ? { _id: id } : { ticketNumber: id }, { $set: update }, { new: true }).lean();
    if (!t) return res.status(404).json({ ok: false, error: 'not_found' });
    return res.json({ ok: true, ticket: t });
  } catch (err) {
    console.error('PUT /api/tickets/:id error', err);
    return res.status(500).json({ ok: false, error: 'update_failed', details: err.message });
  }
});
app.patch('/api/tickets/:id/status', async (req, res) => {
  try {
    const id = req.params.id;
    const { status, reason = '', by = null, meta = {} } = req.body || {};
    if (!status) return res.status(400).json({ ok: false, error: 'status_required' });
    const allowed = ['pending', 'cancelled', 'changed', 'paid'];
    if (!allowed.includes(status)) return res.status(400).json({ ok: false, error: 'invalid_status' });

    const ticket = await (mongoose.Types.ObjectId.isValid(id) ? Ticket.findById(id) : Ticket.findOne({ ticketNumber: id }));
    if (!ticket) return res.status(404).json({ ok: false, error: 'not_found' });

    if (ticket.status === status) return res.json({ ok: true, ticket: ticket.toObject(), note: 'no_change' });

    const prev = ticket.status;
    ticket.statusHistory = ticket.statusHistory || [];
    ticket.statusHistory.push({ from: prev, to: status, by, reason, meta, ts: new Date() });
    ticket.status = status;
    if (status === 'cancelled') ticket.reservationInfo = Object.assign({}, ticket.reservationInfo || {}, { cancelledAt: new Date().toISOString() });
    await ticket.save();

    // best-effort side-effects (release seats/slots) - don't block response
    (async () => {
      try {
        if (status === 'cancelled' && ticket.type === 'bus' && ticket.productId) {
          const SELF_BASE = process.env.SELF_BASE || `http://localhost:${port}`;
          const body = { dateIso: ticket.travelDate, reservationId: ticket.providerReservationId || ticket.orderNumber };
          if (Array.isArray(ticket.seats) && ticket.seats.length) body.seats = ticket.seats;
          await axios.post(`${SELF_BASE}/api/buses/${encodeURIComponent(ticket.productId)}/slots/release`, body, { timeout: 8000 }).catch(() => { });
        }
        if (status === 'cancelled' && ticket.type === 'tour' && ticket.productId) {
          await releaseViaHttp(ticket.productId, ticket.travelDate || ticket.travelStart, ticket.providerReservationId || ticket.orderNumber).catch(() => { });
        }
        // update order timeline
        if (ticket.orderId) {
          try {
            const ord = await Order.findById(ticket.orderId);
            if (ord) {
              ord.timeline = ord.timeline || [];
              ord.timeline.push({ ts: new Date(), text: `Ticket ${ticket.ticketNumber} status -> ${status}`, meta: { ticket: ticket._id } });
              await ord.save();
            }
          } catch (e) { /* ignore */ }
        }
      } catch (e) {
        console.error('ticket status side-effect error', e && e.message ? e.message : e);
      }
    })();

    return res.json({ ok: true, ticket });
  } catch (err) {
    console.error('PATCH /api/tickets/:id/status error', err);
    return res.status(500).json({ ok: false, error: 'status_update_failed', details: err.message });
  }
});

// Delete ticket
app.delete('/api/tickets/:id', async (req, res) => {
  try {
    const id = req.params.id;
    // delete by _id or ticketNumber
    let removed = null;
    if (mongoose.Types.ObjectId.isValid(id)) removed = await Ticket.findByIdAndDelete(id).lean();
    if (!removed) removed = await Ticket.findOneAndDelete({ ticketNumber: id }).lean();
    if (!removed) return res.status(404).json({ ok: false, error: 'not_found' });

    // best-effort: remove ref from order.ticketIds
    try {
      if (removed.orderId) {
        await Order.findByIdAndUpdate(removed.orderId, { $pull: { ticketIds: removed._id } }).catch(() => { });
      } else if (removed.orderNumber) {
        const ord = await Order.findOne({ orderNumber: removed.orderNumber });
        if (ord) await Order.findByIdAndUpdate(ord._id, { $pull: { ticketIds: removed._id } }).catch(() => { });
      }
    } catch (e) { /* ignore */ }

    return res.json({ ok: true, id: removed._id || removed.ticketNumber });
  } catch (err) {
    console.error('DELETE /api/tickets/:id error', err);
    return res.status(500).json({ ok: false, error: 'delete_failed', details: err.message });
  }
});
