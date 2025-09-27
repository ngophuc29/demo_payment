// Node v10.15.3
const axios = require('axios').default; // npm install axios
const CryptoJS = require('crypto-js'); // npm install crypto-js
const express = require('express'); // npm install express
const bodyParser = require('body-parser'); // npm install body-parser
const moment = require('moment'); // npm install moment
const qs = require('qs');
const cors = require('cors')
const app = express();
app.use(cors())

// APP INFO, STK TEST: 4111 1111 1111 1111
const config = {
  app_id: '2554',
  key1: 'sdngKKJmqEMzvh5QQcdD2A9XBSKUNaYn',
  key2: 'trMrHtvjo6myautxDUiAcYsVtaeQ8nhf',
  endpoint: 'https://sb-openapi.zalopay.vn/v2/create',
};

app.use(bodyParser.json());

/**
 * methed: POST
 * Sandbox	POST	https://sb-openapi.zalopay.vn/v2/create
 * Real	POST	https://openapi.zalopay.vn/v2/create
 * description: tạo đơn hàng, thanh toán
 */
app.post('/payment', async (req, res) => {
  const embed_data = {
    //sau khi hoàn tất thanh toán sẽ đi vào link này (thường là link web thanh toán thành công của mình)
    // redirecturl: 'https://www.facebook.com/ngphukk2903/',
    redirecturl: 'http://localhost:3000/thanh-toan-thanh-cong',

  };

  const items = [];
  const transID = Math.floor(Math.random() * 1000000);

  const order = {
    app_id: config.app_id,
    app_trans_id: `${moment().format('YYMMDD')}_${transID}`, // translation missing: vi.docs.shared.sample_code.comments.app_trans_id
    app_user: 'user123',
    app_time: Date.now(), // miliseconds
    item: JSON.stringify(items),
    embed_data: JSON.stringify(embed_data),
    amount: 50000,
    //khi thanh toán xong, zalopay server sẽ POST đến url này để thông báo cho server của mình
    //Chú ý: cần dùng ngrok để public url thì Zalopay Server mới call đến được
    callback_url: 'https://d46b2a98cf42.ngrok-free.app/callback',
    description: `Lazada - Payment for the order #${transID}`,
    bank_code: '',
  };

  // appid|app_trans_id|appuser|amount|apptime|embeddata|item
  const data =
    config.app_id +
    '|' +
    order.app_trans_id +
    '|' +
    order.app_user +
    '|' +
    order.amount +
    '|' +
    order.app_time +
    '|' +
    order.embed_data +
    '|' +
    order.item;
  order.mac = CryptoJS.HmacSHA256(data, config.key1).toString();

  console.log("=== PAYMENT DEBUG ===");
  console.log("MAC input:", data);
  console.log("MAC output:", order.mac);
  console.log("Order:", order);
  console.log("====================");

  try {
    const result = await axios.post(config.endpoint, null, { params: order });

    console.log("Payment response:", result.data);
    return res.status(200).json(result.data);
  } catch (error) {
    console.log("Payment error:", error.response ? error.response.data : error.message);
  }
});

/**
 * method: POST
 * description: callback để Zalopay Server call đến khi thanh toán thành công.
 * Khi và chỉ khi ZaloPay đã thu tiền khách hàng thành công thì mới gọi API này để thông báo kết quả.
 */
app.post('/callback', (req, res) => {
  let result = {};
  console.log("Callback body:", req.body);
  try {
    let dataStr = req.body.data;
    let reqMac = req.body.mac;

    let mac = CryptoJS.HmacSHA256(dataStr, config.key2).toString();
    console.log('mac =', mac);

    // kiểm tra callback hợp lệ (đến từ ZaloPay server)
    if (reqMac !== mac) {
      // callback không hợp lệ
      result.return_code = -1;
      result.return_message = 'mac not equal';
    } else {
      // thanh toán thành công
      // merchant cập nhật trạng thái cho đơn hàng ở đây
      let dataJson = JSON.parse(dataStr, config.key2);
      console.log(
        "update order's status = success where app_trans_id =",
        dataJson['app_trans_id'],
      );

      result.return_code = 1;
      result.return_message = 'success';
    }
  } catch (ex) {
    console.log('lỗi:::' + ex.message);
    result.return_code = 0; // ZaloPay server sẽ callback lại (tối đa 3 lần)
    result.return_message = ex.message;
  }

  // thông báo kết quả cho ZaloPay server
  res.json(result);
});

/**
 * method: POST
 * Sandbox	POST	https://sb-openapi.zalopay.vn/v2/query
 * Real	POST	https://openapi.zalopay.vn/v2/query
 * description:
 * Khi user thanh toán thành công,
 * ZaloPay sẽ gọi callback (notify) tới merchant để merchant cập nhật trạng thái
 * đơn hàng Thành Công trên hệ thống. Trong thực tế callback có thể bị miss do lỗi Network timeout,
 * Merchant Service Unavailable/Internal Error...
 * nên Merchant cần hiện thực việc chủ động gọi API truy vấn trạng thái đơn hàng.
 */
app.post('/check-status-order', async (req, res) => {
  const { app_trans_id } = req.body;

  let postData = {
    app_id: config.app_id,
    app_trans_id, // Input your app_trans_id
  };

  let data = postData.app_id + '|' + postData.app_trans_id + '|' + config.key1; // appid|app_trans_id|key1
  postData.mac = CryptoJS.HmacSHA256(data, config.key1).toString();

  console.log("=== CHECK STATUS DEBUG ===");
  console.log("MAC input:", data);
  console.log("MAC output:", postData.mac);
  console.log("==========================");

  let postConfig = {
    method: 'post',
    url: 'https://sb-openapi.zalopay.vn/v2/query',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    data: qs.stringify(postData),
  };

  try {
    const result = await axios(postConfig);
    console.log("Check status response:", result.data);
    return res.status(200).json(result.data);
  } catch (error) {
    console.log('Check status error:', error.response ? error.response.data : error.message);
  }
});

/**
 * method: POST
 * Sandbox   POST https://sb-openapi.zalopay.vn/v2/refund
 * Real      POST https://openapi.zalopay.vn/v2/refund
 * description: hoàn tiền giao dịch
 */
app.post('/refund', async (req, res) => {
  const { zp_trans_id, amount, description } = req.body;

  const timestamp = Date.now();
  const m_refund_id = `${moment().format('YYMMDD')}_${config.app_id}_${Math.floor(Math.random() * 1000000)}`;

  // mac_input = appid|zp_trans_id|amount|description|timestamp
  const mac_input = `${config.app_id}|${zp_trans_id}|${amount}|${description}|${timestamp}`;
  const mac = CryptoJS.HmacSHA256(mac_input, config.key1).toString();

  console.log("=== REFUND DEBUG ===");
  console.log("MAC input:", mac_input);
  console.log("MAC output:", mac);
  console.log("Refund body:", { app_id: config.app_id, m_refund_id, zp_trans_id, amount, timestamp, description });
  console.log("====================");

  const body = {
    app_id: Number(config.app_id),
    m_refund_id,
    zp_trans_id,
    amount,
    timestamp,
    description,
    mac,
  };

  try {
    const result = await axios.post(
      'https://sb-openapi.zalopay.vn/v2/refund',
      body,
      { headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' } }
    );
    console.log("Refund response:", result.data);
    return res.status(200).json(result.data);
  } catch (error) {
    console.log('Refund error:', error.response ? error.response.data : error.message);
    return res.status(500).json({ error: error.message });
  }
});

/**
 * method: POST
 * Sandbox   POST https://sb-openapi.zalopay.vn/v2/query_refund
 * Real      POST https://openapi.zalopay.vn/v2/query_refund
 * description: kiểm tra trạng thái hoàn tiền
 */
app.post('/query-refund', async (req, res) => {
  const { m_refund_id } = req.body;
  const timestamp = Date.now();

  // app_id|m_refund_id|timestamp
  const data = config.app_id + '|' + m_refund_id + '|' + timestamp;
  const mac = CryptoJS.HmacSHA256(data, config.key1).toString();

  console.log("=== QUERY REFUND DEBUG ===");
  console.log("MAC input:", data);
  console.log("MAC output:", mac);
  console.log("==========================");

  const body = {
    app_id: Number(config.app_id),
    m_refund_id,
    timestamp,
    mac,
  };

  try {
    const result = await axios.post('https://sb-openapi.zalopay.vn/v2/query_refund', body, {
      headers: { 'Content-Type': 'application/json' },
    });

    console.log("Query refund response:", result.data);
    return res.status(200).json(result.data);
  } catch (error) {
    console.log('Query refund error:', error.response ? error.response.data : error.message);
    return res.status(500).json({ error: error.message });
  }
});

app.listen(8888, function () {
  console.log('Server is listening at port :8888');
});
