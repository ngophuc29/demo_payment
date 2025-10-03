const express = require('express')
const app = express()
const port = 7700
const cors = require('cors')
const path = require('path') // <-- existing
const fs = require('fs') // <-- added

// --- New: mongoose for DB CRUD ---
const mongoose = require('mongoose');

// Use provided connection string (or override with env var)
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb+srv://ngophuc2911_db_user:phuc29112003@cluster0.xrujamk.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0';

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
      console.log(`Example app listening on port ${port}`);
    });
  } catch (err) {
    console.error('MongoDB connection error:', err);
    // Exit so the process doesn't accept requests while DB unavailable
    process.exit(1);
  }
})();

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
  price: { type: Number, min: 0 },
  seatsTotal: { type: Number, min: 0, default: 0 },
  seatsAvailable: { type: Number, min: 0, default: 0 },
  // seatMap stored as array of seat objects with status
  seatMap: { type: [SeatSchema], default: [] },
  status: { type: String, enum: ["scheduled", "cancelled", "delayed", "completed"], default: "scheduled" },
  amenities: String,
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now },
}, { collection: 'buses' });

// Validate logical consistency before saving
BusSchema.pre('validate', function (next) {
  try {
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

// --- New: JSON body parsing middleware ---
app.use(express.json());
app.use(cors())

app.get('/', (req, res) => {
  res.send('Hello World!')
})

const airportsData = {
  "airports": {
    "VVBM": {
      "icao": "VVBM",
      "iata": "BMV",
      "name": "Buon Ma Thuot Airport",
      "city": "Buon Ma Thuot",
      "state": "Đắk Lắk",
      "country": "VN",
      "elevation": 1729,
      "lat": 12.668299675,
      "lon": 108.120002747,
      "tz": "Asia/Ho_Chi_Minh"
    },
    "VVCA": {
      "icao": "VVCA",
      "iata": "VCL",
      "name": "Chu Lai International Airport",
      "city": "Dung Quat Bay",
      "state": "Quảng Nam",
      "country": "VN",
      "elevation": 10,
      "lat": 15.4033002853,
      "lon": 108.706001282,
      "tz": "Asia/Ho_Chi_Minh"
    },
    "VVCI": {
      "icao": "VVCI",
      "iata": "HPH",
      "name": "Cat Bi International Airport",
      "city": "Haiphong",
      "state": "Hải Phòng",
      "country": "VN",
      "elevation": 6,
      "lat": 20.8194007874,
      "lon": 106.7249984741,
      "tz": "Asia/Ho_Chi_Minh"
    },
    "VVCL": {
      "icao": "VVCL",
      "iata": "",
      "name": "Cam Ly Airport",
      "city": "",
      "state": "Lâm Đồng",
      "country": "VN",
      "elevation": 4937,
      "lat": 11.9502844865,
      "lon": 108.411004543,
      "tz": "Asia/Ho_Chi_Minh"
    },
    "VVCM": {
      "icao": "VVCM",
      "iata": "CAH",
      "name": "Ca Mau Airport",
      "city": "Ca Mau City",
      "state": "Cà Mau",
      "country": "VN",
      "elevation": 6,
      "lat": 9.1776666667,
      "lon": 105.177777778,
      "tz": "Asia/Ho_Chi_Minh"
    },
    "VVCR": {
      "icao": "VVCR",
      "iata": "CXR",
      "name": "Cam Ranh Airport",
      "city": "Nha Trang",
      "state": "Khánh Hòa",
      "country": "VN",
      "elevation": 40,
      "lat": 11.9982004166,
      "lon": 109.21900177,
      "tz": "Asia/Ho_Chi_Minh"
    },
    "VVCS": {
      "icao": "VVCS",
      "iata": "VCS",
      "name": "Co Ong Airport",
      "city": "Con Ong",
      "state": "Bà Rịa-Vũng Tàu",
      "country": "VN",
      "elevation": 20,
      "lat": 8.7318296433,
      "lon": 106.633003235,
      "tz": "Asia/Ho_Chi_Minh"
    },
    "VVCT": {
      "icao": "VVCT",
      "iata": "VCA",
      "name": "Tra Noc Airport",
      "city": "Can Tho",
      "state": "Cần Thơ",
      "country": "VN",
      "elevation": 9,
      "lat": 10.085100174,
      "lon": 105.7119979858,
      "tz": "Asia/Ho_Chi_Minh"
    },
    "VVDB": {
      "icao": "VVDB",
      "iata": "DIN",
      "name": "Dien Bien Phu Airport",
      "city": "Dien Bien Phu",
      "state": "Điện Biên",
      "country": "VN",
      "elevation": 1611,
      "lat": 21.3974990845,
      "lon": 103.008003235,
      "tz": "Asia/Ho_Chi_Minh"
    },
    "VVDH": {
      "icao": "VVDH",
      "iata": "VDH",
      "name": "Dong Hoi Airport",
      "city": "Dong Hoi",
      "state": "Quảng Bình",
      "country": "VN",
      "elevation": 59,
      "lat": 17.515,
      "lon": 106.590556,
      "tz": "Asia/Ho_Chi_Minh"
    },
    "VVDL": {
      "icao": "VVDL",
      "iata": "DLI",
      "name": "Lien Khuong Airport",
      "city": "Dalat",
      "state": "Lâm Đồng",
      "country": "VN",
      "elevation": 3156,
      "lat": 11.75,
      "lon": 108.3669967651,
      "tz": "Asia/Ho_Chi_Minh"
    },
    "VVDN": {
      "icao": "VVDN",
      "iata": "DAD",
      "name": "Da Nang International Airport",
      "city": "Da Nang",
      "state": "Đà Nẵng",
      "country": "VN",
      "elevation": 33,
      "lat": 16.0438995361,
      "lon": 108.1989974976,
      "tz": "Asia/Ho_Chi_Minh"
    },
    "VVGL": {
      "icao": "VVGL",
      "iata": "",
      "name": "Gia Lam Air Base",
      "city": "Hanoi",
      "state": "Hà Nội",
      "country": "VN",
      "elevation": 50,
      "lat": 21.0405006409,
      "lon": 105.8860015869,
      "tz": "Asia/Ho_Chi_Minh"
    },
    "VVKP": {
      "icao": "VVKP",
      "iata": "",
      "name": "Kep Air Base",
      "city": "Kep",
      "state": "Bắc Giang",
      "country": "VN",
      "elevation": 55,
      "lat": 21.3945999146,
      "lon": 106.261001587,
      "tz": "Asia/Ho_Chi_Minh"
    },
    "VVNB": {
      "icao": "VVNB",
      "iata": "HAN",
      "name": "Noi Bai International Airport",
      "city": "Hanoi",
      "state": "Hà Nội",
      "country": "VN",
      "elevation": 39,
      "lat": 21.221200943,
      "lon": 105.8069992065,
      "tz": "Asia/Ho_Chi_Minh"
    },
    "VVNS": {
      "icao": "VVNS",
      "iata": "SQH",
      "name": "Na-San Airport",
      "city": "Son La",
      "state": "Sơn La",
      "country": "VN",
      "elevation": 2133,
      "lat": 21.216999054,
      "lon": 104.0329971313,
      "tz": "Asia/Ho_Chi_Minh"
    },
    "VVNT": {
      "icao": "VVNT",
      "iata": "NHA",
      "name": "Nha Trang Air Base",
      "city": "Nha Trang",
      "state": "Khánh Hòa",
      "country": "VN",
      "elevation": 20,
      "lat": 12.2274999619,
      "lon": 109.192001343,
      "tz": "Asia/Ho_Chi_Minh"
    },
    "VVPB": {
      "icao": "VVPB",
      "iata": "HUI",
      "name": "Phu Bai Airport",
      "city": "Hue",
      "state": "Thừa Thiên-Huế",
      "country": "VN",
      "elevation": 48,
      "lat": 16.4015007019,
      "lon": 107.70300293,
      "tz": "Asia/Ho_Chi_Minh"
    },
    "VVPC": {
      "icao": "VVPC",
      "iata": "UIH",
      "name": "Phu Cat Airport",
      "city": "Quy Nhon",
      "state": "Bình Định",
      "country": "VN",
      "elevation": 80,
      "lat": 13.9549999237,
      "lon": 109.041999817,
      "tz": "Asia/Ho_Chi_Minh"
    },
    "VVPK": {
      "icao": "VVPK",
      "iata": "PXU",
      "name": "Pleiku Airport",
      "city": "Pleiku",
      "state": "Gia Lai",
      "country": "VN",
      "elevation": 2434,
      "lat": 14.0045003891,
      "lon": 108.016998291,
      "tz": "Asia/Ho_Chi_Minh"
    },
    "VVPQ": {
      "icao": "VVPQ",
      "iata": "PQC",
      "name": "Phu Quoc Airport",
      "city": "Duong Dong",
      "state": "Kiên Giang",
      "country": "VN",
      "elevation": 23,
      "lat": 10.2270002365,
      "lon": 103.967002869,
      "tz": "Asia/Ho_Chi_Minh"
    },
    "VVPR": {
      "icao": "VVPR",
      "iata": "PHA",
      "name": "Phan Rang Airport",
      "city": "Phan Rang",
      "state": "Ninh Thuận",
      "country": "VN",
      "elevation": 101,
      "lat": 11.6335000992,
      "lon": 108.952003479,
      "tz": "Asia/Ho_Chi_Minh"
    },
    "VVPT": {
      "icao": "VVPT",
      "iata": "",
      "name": "Phan Thiet Airport",
      "city": "Phan Thiet",
      "state": "Bình Thuận",
      "country": "VN",
      "elevation": 0,
      "lat": 10.9063997269,
      "lon": 108.0690002441,
      "tz": "Asia/Ho_Chi_Minh"
    },
    "VVRG": {
      "icao": "VVRG",
      "iata": "VKG",
      "name": "Rach Gia Airport",
      "city": "Rach Gia",
      "state": "Kiên Giang",
      "country": "VN",
      "elevation": 7,
      "lat": 9.9580299723,
      "lon": 105.132379532,
      "tz": "Asia/Ho_Chi_Minh"
    },
    "VVTH": {
      "icao": "VVTH",
      "iata": "TBB",
      "name": "Dong Tac Airport",
      "city": "Tuy Hoa",
      "state": "Phú Yên",
      "country": "VN",
      "elevation": 20,
      "lat": 13.0495996475,
      "lon": 109.333999634,
      "tz": "Asia/Ho_Chi_Minh"
    },
    "VVTS": {
      "icao": "VVTS",
      "iata": "SGN",
      "name": "Tan Son Nhat International Airport",
      "city": "Ho Chi Minh City",
      "state": "Hồ Chí Minh",
      "country": "VN",
      "elevation": 33,
      "lat": 10.8187999725,
      "lon": 106.652000427,
      "tz": "Asia/Ho_Chi_Minh"
    },
    "VVTX": {
      "icao": "VVTX",
      "iata": "THD",
      "name": "Thọ Xuân Airport",
      "city": "Thanh Hóa",
      "state": "Thanh Hóa",
      "country": "VN",
      "elevation": 59,
      "lat": 19.901667,
      "lon": 105.467778,
      "tz": "Asia/Ho_Chi_Minh"
    },
    "VVVD": {
      "icao": "VVVD",
      "iata": "VDO",
      "name": "Van Don International Airport",
      "city": "Vân Đồn",
      "state": "Quảng Ninh",
      "country": "VN",
      "elevation": 26,
      "lat": 21.117778,
      "lon": 107.414167,
      "tz": "Asia/Ho_Chi_Minh"
    },
    "VVVH": {
      "icao": "VVVH",
      "iata": "VII",
      "name": "Vinh Airport",
      "city": "Vinh",
      "state": "Nghệ An",
      "country": "VN",
      "elevation": 23,
      "lat": 18.7376003265,
      "lon": 105.67099762,
      "tz": "Asia/Ho_Chi_Minh"
    },
    "VVVT": {
      "icao": "VVVT",
      "iata": "VTG",
      "name": "Vung Tau Airport",
      "city": "Vung Tau",
      "state": "Bà Rịa-Vũng Tàu",
      "country": "VN",
      "elevation": 13,
      "lat": 10.3725004196,
      "lon": 107.0950012207,
      "tz": "Asia/Ho_Chi_Minh"
    }
  }
};

app.get('/airports', (req, res) => {
  console.log('GET /airports called');
  res.json(airportsData);
})

// thêm đường dẫn tới 2 file JSON (ở thư mục cha)
const busRawFile = path.join(__dirname, '..', 'cac_ben_xe_bus_chua_sat_nhap.json')
const busNormalizedFile = path.join(__dirname, '..', 'cac_ben_xe_bus_sau_sat_nhap.json')

// --- New: đường dẫn tới file nhà xe ---
const nhaxeFile = path.join(__dirname, '..', 'nhaxekhach.json')

// --- New: đường dẫn tới file loại xe (loaixe.json) ---
const loaixeFile = path.join(__dirname, '..', 'dsloaixevexere.json')

// route trả về file JSON "chua_sat_nhap" - now reads, logs length if array, returns JSON
app.get('/bus/cac_ben_xe_bus_chua_sat_nhap', (req, res) => {
  console.log('GET /bus/cac_ben_xe_bus_chua_sat_nhap called');
  fs.readFile(busRawFile, 'utf8', (err, data) => {
    if (err) {
      console.error('Error reading cac_ben_xe_bus_chua_sat_nhap.json:', err);
      return res.status(500).json({ error: 'Failed to read file' });
    }
    let parsed;
    try {
      parsed = JSON.parse(data);
      if (Array.isArray(parsed)) {
        console.log(`cac_ben_xe_bus_chua_sat_nhap: array length = ${parsed.length}`);
      } else if (parsed && parsed.stations && Array.isArray(parsed.stations)) {
        console.log(`cac_ben_xe_bus_chua_sat_nhap: stations length = ${parsed.stations.length}`);
      } else {
        console.log('cac_ben_xe_bus_chua_sat_nhap: parsed but not an array');
      }
    } catch (e) {
      console.warn('Could not parse cac_ben_xe_bus_chua_sat_nhap.json:', e);
      return res.status(500).json({ error: 'Failed to parse JSON' });
    }
    res.json(parsed);
  });
});

// route trả về file JSON "sau_sat_nhap" - now reads, logs length if array, returns JSON
app.get('/bus/cac_ben_xe_bus_sau_sat_nhap', (req, res) => {
  console.log('GET /bus/cac_ben_xe_bus_sau_sat_nhap called');
  fs.readFile(busNormalizedFile, 'utf8', (err, data) => {
    if (err) {
      console.error('Error reading cac_ben_xe_bus_sau_sat_nhap.json:', err);
      return res.status(500).json({ error: 'Failed to read file' });
    }
    let parsed;
    try {
      parsed = JSON.parse(data);
      if (Array.isArray(parsed)) {
        console.log(`cac_ben_xe_bus_sau_sat_nhap: array length = ${parsed.length}`);
      } else if (parsed && parsed.stations && Array.isArray(parsed.stations)) {
        console.log(`cac_ben_xe_bus_sau_sat_nhap: stations length = ${parsed.stations.length}`);
      } else {
        console.log('cac_ben_xe_bus_sau_sat_nhap: parsed but not an array');
      }
    } catch (e) {
      console.warn('Could not parse cac_ben_xe_bus_sau_sat_nhap.json:', e);
      return res.status(500).json({ error: 'Failed to parse JSON' });
    }
    res.json(parsed);
  });
})

// --- New: route trả về danh sách nhà xe từ nhaxekhach.json ---
app.get('/bus/nhaxe', (req, res) => {
  console.log('GET /nhaxe called');
  fs.readFile(nhaxeFile, 'utf8', (err, data) => {
    if (err) {
      console.error('Error reading nhaxekhach.json:', err);
      return res.status(500).json({ error: 'Failed to read file' });
    }
    try {
      const parsed = JSON.parse(data);
      if (Array.isArray(parsed)) {
        console.log(`nhaxekhach: array length = ${parsed.length}`);
      } else {
        console.log('nhaxekhach: parsed but not an array');
      }
      return res.json(parsed);
    } catch (e) {
      console.warn('Could not parse nhaxekhach.json:', e);
      return res.status(500).json({ error: 'Failed to parse JSON' });
    }
  });
})

// --- New: route trả về toàn bộ loại xe ---
app.get('/bus/loaixe', (req, res) => {
  console.log('GET /loaixe called');
  fs.readFile(loaixeFile, 'utf8', (err, data) => {
    if (err) {
      console.error('Error reading loaixe.json:', err);
      return res.status(500).json({ error: 'Failed to read file' });
    }
    try {
      const parsed = JSON.parse(data);
      // prefer vehicle_categories key used in dsloaixevexere.json
      const list = parsed.vehicle_categories || parsed.vehicle_types || parsed;
      if (Array.isArray(list)) {
        console.log(`loaixe: vehicle_categories length = ${list.length}`);
      } else {
        console.log('loaixe: parsed but vehicle_categories not an array');
      }
      return res.json(list);
    } catch (e) {
      console.warn('Could not parse loaixe.json:', e);
      return res.status(500).json({ error: 'Failed to parse JSON' });
    }
  });
});

// --- New: route trả về 1 loại xe theo id ---
app.get('/bus/loaixe/:id', (req, res) => {
  const id = req.params.id;
  console.log(`GET /loaixe/${id} called`);
  fs.readFile(loaixeFile, 'utf8', (err, data) => {
    if (err) {
      console.error('Error reading loaixe.json:', err);
      return res.status(500).json({ error: 'Failed to read file' });
    }
    try {
      const parsed = JSON.parse(data);
      // prefer vehicle_categories key
      const list = parsed.vehicle_categories || parsed.vehicle_types || [];
      const item = list.find(v => v.id === id);
      if (!item) {
        return res.status(404).json({ error: 'Vehicle type not found' });
      }
      return res.json(item);
    } catch (e) {
      console.warn('Could not parse loaixe.json:', e);
      return res.status(500).json({ error: 'Failed to parse JSON' });
    }
  });
});

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
    const buses = await Bus.find(filter)
      .sort({ departureAt: 1 })
      .skip((page - 1) * pageSize)
      .limit(pageSize)
      .lean();

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

    // Create and save
    const bus = new Bus(payload);
    await bus.save();
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

    payload.updatedAt = new Date();
    const bus = await Bus.findByIdAndUpdate(req.params.id, payload, { new: true, runValidators: true }).lean();
    if (!bus) return res.status(404).json({ error: 'Bus not found' });
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
    for (let i = 0; i < bus.seatMap.length; i++) {
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

    // from / to filtering: try to match code / city / name (case-insensitive, partial)
    if (from) {
      const re = new RegExp(from, 'i');
      filter.$and = filter.$and || [];
      filter.$and.push({
        $or: [
          { 'routeFrom.code': re },
          { 'routeFrom.city': re },
          { 'routeFrom.name': re },
          { 'routeFrom.id': re }
        ]
      });
    }
    if (to) {
      const re = new RegExp(to, 'i');
      filter.$and = filter.$and || [];
      filter.$and.push({
        $or: [
          { 'routeTo.code': re },
          { 'routeTo.city': re },
          { 'routeTo.name': re },
          { 'routeTo.id': re }
        ]
      });
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
      const d = new Date(departure);
      if (!isNaN(d.getTime())) {
        depStart = new Date(d);
        depStart.setHours(0, 0, 0, 0);
        depEnd = new Date(depStart);
        depEnd.setDate(depEnd.getDate() + 1);
        // match either departureAt in day OR at least one element in departureDates in day
        filter.$and = filter.$and || [];
        filter.$and.push({
          $or: [
            { departureAt: { $gte: depStart, $lt: depEnd } },
            { departureDates: { $elemMatch: { $gte: depStart, $lt: depEnd } } }
          ]
        });
      }
    }

    // Debug log to help diagnose why nothing matched
    console.log('GET /api/client/buses - filter:', JSON.stringify(filter));
    if (depStart && depEnd) {
      console.log('departure range:', depStart.toISOString(), depEnd.toISOString());
    }

    // count and fetch with projection suitable for client listing
    let total = await Bus.countDocuments(filter);
    let buses = await Bus.find(filter)
      .sort({ departureAt: 1 })
      .skip((page - 1) * pageSize)
      .limit(pageSize)
      .select('busCode operator routeFrom routeTo departureAt departureDates arrivalAt arrivalDates price seatsAvailable seatsTotal duration busType status amenities seatMap')
      .lean();

    // If strict filter returned nothing but user provided from/to, try a relaxed fallback:
    if ((Array.isArray(buses) && buses.length === 0) && (from || to)) {
      const relaxedAnd = [];
      if (from) {
        const reFrom = new RegExp(from, 'i');
        relaxedAnd.push({
          $or: [
            { 'routeFrom.code': reFrom }, { 'routeFrom.city': reFrom }, { 'routeFrom.name': reFrom },
            { 'routeTo.code': reFrom }, { 'routeTo.city': reFrom }, { 'routeTo.name': reFrom }
          ]
        });
      }
      if (to) {
        const reTo = new RegExp(to, 'i');
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
      console.log('relaxed fallback filter:', JSON.stringify(relaxedFilter));
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
