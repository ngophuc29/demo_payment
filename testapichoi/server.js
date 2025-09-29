const express = require('express')
const app = express()
const port = 7700
const cors = require('cors')
const path = require('path') // <-- existing
const fs = require('fs') // <-- added

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
const busRawFile = path.join(__dirname, '..', 'vietnam_bus_chua_sat_nhap.json')
const busNormalizedFile = path.join(__dirname, '..', 'vietnam_bus_sau_sat_nhap.json')

// route trả về file JSON "chua_sat_nhap" - now reads, logs length if array, returns JSON
app.get('/bus/vietnam_bus_chua_sat_nhap', (req, res) => {
  console.log('GET /bus/vietnam_bus_chua_sat_nhap called');
  fs.readFile(busRawFile, 'utf8', (err, data) => {
    if (err) {
      console.error('Error reading vietnam_bus_chua_sat_nhap.json:', err);
      return res.status(500).json({ error: 'Failed to read file' });
    }
    let parsed;
    try {
      parsed = JSON.parse(data);
      if (Array.isArray(parsed)) {
        console.log(`vietnam_bus_chua_sat_nhap: array length = ${parsed.length}`);
      } else if (parsed && parsed.stations && Array.isArray(parsed.stations)) {
        console.log(`vietnam_bus_chua_sat_nhap: stations length = ${parsed.stations.length}`);
      } else {
        console.log('vietnam_bus_chua_sat_nhap: parsed but not an array');
      }
    } catch (e) {
      console.warn('Could not parse vietnam_bus_chua_sat_nhap.json:', e);
      return res.status(500).json({ error: 'Failed to parse JSON' });
    }
    res.json(parsed);
  });
});

// route trả về file JSON "sau_sat_nhap" - now reads, logs length if array, returns JSON
app.get('/bus/vietnam_bus_sau_sat_nhap', (req, res) => {
  console.log('GET /bus/vietnam_bus_sau_sat_nhap called');
  fs.readFile(busNormalizedFile, 'utf8', (err, data) => {
    if (err) {
      console.error('Error reading vietnam_bus_sau_sat_nhap.json:', err);
      return res.status(500).json({ error: 'Failed to read file' });
    }
    let parsed;
    try {
      parsed = JSON.parse(data);
      if (Array.isArray(parsed)) {
        console.log(`vietnam_bus_sau_sat_nhap: array length = ${parsed.length}`);
      } else if (parsed && parsed.stations && Array.isArray(parsed.stations)) {
        console.log(`vietnam_bus_sau_sat_nhap: stations length = ${parsed.stations.length}`);
      } else {
        console.log('vietnam_bus_sau_sat_nhap: parsed but not an array');
      }
    } catch (e) {
      console.warn('Could not parse vietnam_bus_sau_sat_nhap.json:', e);
      return res.status(500).json({ error: 'Failed to parse JSON' });
    }
    res.json(parsed);
  });
})

app.listen(port, () => {
  console.log(`Example app listening on port ${port}`)
})
