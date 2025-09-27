// Tổng hợp API MoMo, ZaloPay, VNPay
const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const axios = require('axios');
const crypto = require('crypto');
const moment = require('moment');
const qs = require('qs');
const CryptoJS = require('crypto-js');
const { VNPay, ignoreLogger, ProductCode, VnpLocale, dateFormat } = require('vnpay');

const momoConfig = require('./momo/config');

const app = express();
app.use(cors());
app.use(express.json());
app.use(bodyParser.urlencoded({ extended: true }));

// ========== MoMo API ==========
app.post('/momo/payment', async (req, res) => {
    console.log('[MoMo] Request body:', req.body);
    let {
        accessKey,
        secretKey,
        orderInfo,
        partnerCode,
        redirectUrl,
        ipnUrl,
        requestType,
        extraData,
        orderGroupId,
        autoCapture,
        lang,
    } = momoConfig;
    var amount = '10000';
    var orderId = partnerCode + new Date().getTime();
    var requestId = orderId;
    var rawSignature =
        'accessKey=' + accessKey +
        '&amount=' + amount +
        '&extraData=' + extraData +
        '&ipnUrl=' + ipnUrl +
        '&orderId=' + orderId +
        '&orderInfo=' + orderInfo +
        '&partnerCode=' + partnerCode +
        '&redirectUrl=' + redirectUrl +
        '&requestId=' + requestId +
        '&requestType=' + requestType;
    var signature = crypto.createHmac('sha256', secretKey).update(rawSignature).digest('hex');
    const requestBody = JSON.stringify({
        partnerCode,
        partnerName: 'Test',
        storeId: 'MomoTestStore',
        requestId,
        amount,
        orderId,
        orderInfo,
        redirectUrl,
        ipnUrl,
        lang,
        requestType,
        autoCapture,
        extraData,
        orderGroupId,
        signature,
    });
    const options = {
        method: 'POST',
        url: 'https://test-payment.momo.vn/v2/gateway/api/create',
        headers: {
            'Content-Type': 'application/json',
            'Content-Length': Buffer.byteLength(requestBody),
        },
        data: requestBody,
    };
    try {
        const result = await axios(options);
        console.log('[MoMo] /payment response:', result.data);
        return res.status(200).json(result.data);
    } catch (error) {
        console.log('[MoMo] /payment error:', error.response ? error.response.data : error.message);
        return res.status(500).json({ statusCode: 500, message: error.message });
    }
});

app.post('/momo/callback', async (req, res) => {
    console.log('[MoMo] Request body:', req.body);
    console.log('[MoMo] callback:', req.body);
    return res.status(204).json(req.body);
});

app.post('/momo/check-status-transaction', async (req, res) => {
    console.log('[MoMo] Request body:', req.body);
    const { orderId } = req.body;
    var secretKey = momoConfig.secretKey;
    var accessKey = momoConfig.accessKey;
    const rawSignature = `accessKey=${accessKey}&orderId=${orderId}&partnerCode=MOMO&requestId=${orderId}`;
    const signature = crypto.createHmac('sha256', secretKey).update(rawSignature).digest('hex');
    const requestBody = JSON.stringify({
        partnerCode: 'MOMO',
        requestId: orderId,
        orderId: orderId,
        signature: signature,
        lang: 'vi',
    });
    const options = {
        method: 'POST',
        url: 'https://test-payment.momo.vn/v2/gateway/api/query',
        headers: { 'Content-Type': 'application/json' },
        data: requestBody,
    };
    try {
        const result = await axios(options);
        console.log('[MoMo] /check-status-transaction response:', result.data);
        return res.status(200).json(result.data);
    } catch (error) {
        console.log('[MoMo] /check-status-transaction error:', error.response ? error.response.data : error.message);
        return res.status(500).json({ statusCode: 500, message: error.message });
    }
});

app.post('/momo/refund', async (req, res) => {
    console.log('[MoMo] Request body:', req.body);
    const { orderId, amount, transId, description } = req.body;
    const { partnerCode, accessKey, secretKey, lang } = momoConfig;
    const requestId = partnerCode + Date.now();
    const rawSignature =
        'accessKey=' + accessKey +
        '&amount=' + amount +
        '&description=' + description +
        '&orderId=' + orderId +
        '&partnerCode=' + partnerCode +
        '&requestId=' + requestId +
        '&transId=' + transId;
    const signature = crypto.createHmac('sha256', secretKey).update(rawSignature).digest('hex');
    const requestBody = JSON.stringify({
        partnerCode: partnerCode,
        orderId: orderId,
        requestId: requestId,
        amount: amount,
        transId: transId,
        lang: lang || 'vi',
        description: description || '',
        signature: signature,
    });
    const options = {
        method: 'POST',
        url: 'https://test-payment.momo.vn/v2/gateway/api/refund',
        headers: {
            'Content-Type': 'application/json',
            'Content-Length': Buffer.byteLength(requestBody),
        },
        data: requestBody,
        timeout: 30000,
    };
    try {
        const result = await axios(options);
        console.log('[MoMo] /refund response:', result.data);
        return res.status(200).json(result.data);
    } catch (error) {
        console.log('[MoMo] /refund error:', error.response ? error.response.data : error.message);
        return res.status(500).json({ statusCode: 500, message: error.message });
    }
});


// ========== ZaloPay API ==========
const zaloConfig = {
    app_id: '2554',
    key1: 'sdngKKJmqEMzvh5QQcdD2A9XBSKUNaYn',
    key2: 'trMrHtvjo6myautxDUiAcYsVtaeQ8nhf',
    endpoint: 'https://sb-openapi.zalopay.vn/v2/create',
};

app.post('/zalo/payment', async (req, res) => {
    console.log('[ZaloPay] Request body:', req.body);
    const embed_data = { redirecturl: 'http://localhost:3000/thanh-toan-thanh-cong' };
    const items = [];
    const transID = Math.floor(Math.random() * 1000000);
    const order = {
        app_id: zaloConfig.app_id,
        app_trans_id: `${moment().format('YYMMDD')}_${transID}`,
        app_user: 'user123',
        app_time: Date.now(),
        item: JSON.stringify(items),
        embed_data: JSON.stringify(embed_data),
        amount: 50000,
        callback_url: 'https://d46b2a98cf42.ngrok-free.app/callback',
        description: `Lazada - Payment for the order #${transID}`,
        bank_code: '',
    };
    const data =
        zaloConfig.app_id + '|' + order.app_trans_id + '|' + order.app_user + '|' + order.amount + '|' + order.app_time + '|' + order.embed_data + '|' + order.item;
    order.mac = CryptoJS.HmacSHA256(data, zaloConfig.key1).toString();
    console.log('[ZaloPay] PAYMENT DEBUG');
    console.log('[ZaloPay] MAC input:', data);
    console.log('[ZaloPay] MAC output:', order.mac);
    console.log('[ZaloPay] Order:', order);
    try {
        const result = await axios.post(zaloConfig.endpoint, null, { params: order });
        console.log('[ZaloPay] Payment response:', result.data);
        return res.status(200).json(result.data);
    } catch (error) {
        console.log('[ZaloPay] Payment error:', error.response ? error.response.data : error.message);
        return res.status(500).json({ error: error.message });
    }
});

app.post('/zalo/callback', (req, res) => {
    console.log('[ZaloPay] Request body:', req.body);
    let result = {};
    console.log('[ZaloPay] Callback body:', req.body);
    try {
        let dataStr = req.body.data;
        let reqMac = req.body.mac;
        let mac = CryptoJS.HmacSHA256(dataStr, zaloConfig.key2).toString();
        if (reqMac !== mac) {
            result.return_code = -1;
            result.return_message = 'mac not equal';
        } else {
            let dataJson = JSON.parse(dataStr);
            console.log("[ZaloPay] update order's status = success where app_trans_id =", dataJson['app_trans_id']);
            result.return_code = 1;
            result.return_message = 'success';
        }
    } catch (ex) {
        console.log('[ZaloPay] callback error:', ex.message);
        result.return_code = 0;
        result.return_message = ex.message;
    }
    res.json(result);
});

app.post('/zalo/check-status-order', async (req, res) => {
    console.log('[ZaloPay] Request body:', req.body);
    const { app_trans_id } = req.body;
    let postData = {
        app_id: zaloConfig.app_id,
        app_trans_id,
    };
    let data = postData.app_id + '|' + postData.app_trans_id + '|' + zaloConfig.key1;
    postData.mac = CryptoJS.HmacSHA256(data, zaloConfig.key1).toString();
    let postConfig = {
        method: 'post',
        url: 'https://sb-openapi.zalopay.vn/v2/query',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        data: qs.stringify(postData),
    };
    try {
        const result = await axios(postConfig);
        console.log('[ZaloPay] Check status response:', result.data);
        return res.status(200).json(result.data);
    } catch (error) {
        console.log('[ZaloPay] Check status error:', error.response ? error.response.data : error.message);
        return res.status(500).json({ error: error.message });
    }
});

app.post('/zalo/refund', async (req, res) => {
    console.log('[ZaloPay] Request body:', req.body);
    const { zp_trans_id, amount, description } = req.body;
    const timestamp = Date.now();
    const m_refund_id = `${moment().format('YYMMDD')}_${zaloConfig.app_id}_${Math.floor(Math.random() * 1000000)}`;
    console.log('[ZaloPay] Generated m_refund_id:', m_refund_id);
    const mac_input = `${zaloConfig.app_id}|${zp_trans_id}|${amount}|${description}|${timestamp}`;
    const mac = CryptoJS.HmacSHA256(mac_input, zaloConfig.key1).toString();
    const body = {
        app_id: Number(zaloConfig.app_id),
        m_refund_id,
        zp_trans_id,
        amount,
        timestamp,
        description,
        mac,
    };
    try {
        const result = await axios.post('https://sb-openapi.zalopay.vn/v2/refund', body, {
            headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
        });
        console.log('[ZaloPay] Refund response:', result.data);
        return res.status(200).json(result.data);
    } catch (error) {
        console.log('[ZaloPay] Refund error:', error.response ? error.response.data : error.message);
        return res.status(500).json({ error: error.message });
    }
});

app.post('/zalo/query-refund', async (req, res) => {
    console.log('[ZaloPay] Request body:', req.body);
    const { m_refund_id } = req.body;
    const timestamp = Date.now();
    const data = zaloConfig.app_id + '|' + m_refund_id + '|' + timestamp;
    const mac = CryptoJS.HmacSHA256(data, zaloConfig.key1).toString();
    const body = {
        app_id: Number(zaloConfig.app_id),
        m_refund_id,
        timestamp,
        mac,
    };
    try {
        const result = await axios.post('https://sb-openapi.zalopay.vn/v2/query_refund', body, {
            headers: { 'Content-Type': 'application/json' },
        });
        console.log('[ZaloPay] Query refund response:', result.data);
        return res.status(200).json(result.data);
    } catch (error) {
        console.log('[ZaloPay] Query refund error:', error.response ? error.response.data : error.message);
        return res.status(500).json({ error: error.message });
    }
});

// ========== VNPay API ========== 
const vnpayConfig = {
    tmnCode: 'EMDDQ6MT',
    secureSecret: 'Y2AF1I0YYYFU925T8CWI0ZF46P4CJRY8',
    vnpayHost: 'https://sandbox.vnpayment.vn',
    testMode: true,
    hashAlgorithm: 'SHA512',
};

// Tạo link thanh toán VNPay (POST)
app.post('/vnpay/create_payment_url', async (req, res) => {
    const vnpay = new VNPay({ ...vnpayConfig, loggerFn: ignoreLogger });
    const tomorrow = new Date();
    tomorrow.setDate(tomorrow.getDate() + 1);
    const vnpayResponse = await vnpay.buildPaymentUrl({
        vnp_Amount: 50000,
        vnp_IpAddr: '127.0.0.1',
        vnp_TxnRef: `${Date.now()}${Math.floor(Math.random() * 1000)}`,
        vnp_OrderInfo: '1234569',
        vnp_OrderType: ProductCode.Other,
        vnp_ReturnUrl: 'http://localhost:3002/vnpay/check-payment',
        vnp_Locale: VnpLocale.VN,
        vnp_CreateDate: dateFormat(new Date()),
        vnp_ExpireDate: dateFormat(tomorrow),
    });
    const urlObj = new URL(vnpayResponse);
    const params = urlObj.searchParams;
    console.log('--- Thông tin giao dịch tạo mới ---');
    console.log('vnp_TxnRef:', params.get('vnp_TxnRef'));
    console.log('vnp_Amount:', params.get('vnp_Amount'));
    console.log('vnp_CreateDate:', params.get('vnp_CreateDate'));
    console.log('vnp_OrderInfo:', params.get('vnp_OrderInfo'));
    console.log('paymentUrl:', vnpayResponse);
    console.log('-----------------------------------');
    return res.status(201).json(vnpayResponse);
});

// Query trạng thái thanh toán: POST /vnpay/querydr
app.post('/vnpay/querydr', async (req, res) => {
    try {
        const { vnp_TxnRef, vnp_TransactionDate, vnp_OrderInfo } = req.body;
        const vnp_RequestId = `${Date.now()}${Math.floor(Math.random() * 1000)}`;
        const vnp_Version = '2.1.0';
        const vnp_Command = 'querydr';
        const vnp_TmnCode = vnpayConfig.tmnCode;
        const vnp_CreateDate = new Date().toISOString().replace(/[-T:.Z]/g, '').slice(0, 14);
        const vnp_IpAddr = req.ip || '127.0.0.1';
        const vnpayHost = 'https://sandbox.vnpayment.vn/merchant_webapi/api/transaction';
        const secretKey = vnpayConfig.secureSecret;
        const dataToHash = [
            vnp_RequestId,
            vnp_Version,
            vnp_Command,
            vnp_TmnCode,
            vnp_TxnRef,
            vnp_TransactionDate,
            vnp_CreateDate,
            vnp_IpAddr,
            vnp_OrderInfo
        ].join('|');
        const vnp_SecureHash = crypto.createHmac('sha512', secretKey).update(dataToHash).digest('hex');
        const payload = {
            vnp_RequestId,
            vnp_Version,
            vnp_Command,
            vnp_TmnCode,
            vnp_TxnRef,
            vnp_TransactionDate,
            vnp_CreateDate,
            vnp_IpAddr,
            vnp_OrderInfo,
            vnp_SecureHash
        };
        console.log('Payload gửi sang VNPAY (querydr):', payload);
        const response = await axios.post(vnpayHost, payload, {
            headers: { 'Content-Type': 'application/json' }
        });
        console.log('--- Kết quả querydr ---');
        console.log('vnp_TxnRef:', response.data.vnp_TxnRef);
        console.log('vnp_TransactionDate:', response.data.vnp_TransactionDate);
        console.log('vnp_Amount:', response.data.vnp_Amount);
        console.log('vnp_OrderInfo:', response.data.vnp_OrderInfo);
        console.log('vnp_TransactionNo:', response.data.vnp_TransactionNo);
        console.log('vnp_TransactionType:', response.data.vnp_TransactionType);
        console.log('vnp_TransactionStatus:', response.data.vnp_TransactionStatus);
        console.log('---------------------------------------------------');
        return res.json(response.data);
    } catch (err) {
        console.error('Lỗi querydr:', err.message, err.response?.data);
        return res.status(500).json({ error: err.message, detail: err.response?.data });
    }
});

// Refund: POST /vnpay/refund
app.post('/vnpay/refund', async (req, res) => {
    try {
        const {
            vnp_TxnRef,
            vnp_Amount,
            vnp_TransactionType,
            vnp_TransactionDate,
            vnp_OrderInfo,
            vnp_CreateBy,
            vnp_TransactionNo
        } = req.body;
        const vnp_RequestId = `${Date.now()}${Math.floor(Math.random() * 1000)}`;
        const vnp_Version = '2.1.0';
        const vnp_Command = 'refund';
        const vnp_TmnCode = vnpayConfig.tmnCode;
        const vnp_CreateDate = new Date().toISOString().replace(/[-T:.Z]/g, '').slice(0, 14);
        const vnp_IpAddr = req.ip || '127.0.0.1';
        const vnpayHost = 'https://sandbox.vnpayment.vn/merchant_webapi/api/transaction';
        const secretKey = vnpayConfig.secureSecret;
        const vnp_TransactionNoVal = vnp_TransactionNo || '';
        const dataToHash = [
            vnp_RequestId,
            vnp_Version,
            vnp_Command,
            vnp_TmnCode,
            vnp_TransactionType,
            vnp_TxnRef,
            vnp_Amount,
            vnp_TransactionNoVal,
            vnp_TransactionDate,
            vnp_CreateBy,
            vnp_CreateDate,
            vnp_IpAddr,
            vnp_OrderInfo
        ].join('|');
        const vnp_SecureHash = crypto.createHmac('sha512', secretKey).update(dataToHash).digest('hex');
        const payload = {
            vnp_RequestId,
            vnp_Version,
            vnp_Command,
            vnp_TmnCode,
            vnp_TransactionType,
            vnp_TxnRef,
            vnp_Amount,
            vnp_TransactionNo: vnp_TransactionNoVal,
            vnp_TransactionDate,
            vnp_CreateBy,
            vnp_CreateDate,
            vnp_IpAddr,
            vnp_OrderInfo,
            vnp_SecureHash
        };
        console.log('Payload gửi sang VNPAY (refund):', payload);
        const response = await axios.post(vnpayHost, payload, {
            headers: { 'Content-Type': 'application/json' }
        });
        console.log('Response từ VNPAY (refund):', response.data);
        return res.json(response.data);
    } catch (err) {
        console.error('Lỗi refund:', err.message, err.response?.data);
        return res.status(500).json({ error: err.message, detail: err.response?.data });
    }
});

// Check trạng thái refund: POST /vnpay/check-refund-status
// Replace your existing /vnpay/check-refund-status with this
app.post('/vnpay/check-refund-status', async (req, res) => {
    try {
        // EXPECTED: client gửi lên merchant txn ref (vnp_TxnRef) — KHÔNG GỬI vnp_TransactionNo thay vào
        const { vnp_TxnRef, vnp_PayDate, vnp_OrderInfo } = req.body;

        if (!vnp_TxnRef) {
            return res.status(400).json({ error: 'vnp_TxnRef (merchant transaction reference) is required. Do not send VNPAY transactionNo here.' });
        }

        const vnp_RequestId = `${Date.now()}${Math.floor(Math.random() * 1000)}`;
        const vnp_Version = '2.1.0';
        const vnp_Command = 'querydr';
        const vnp_TmnCode = vnpayConfig.tmnCode;
        // vnp_TransactionDate: nếu client biết thời gian của giao dịch thì gửi lên (yyyyMMddHHmmss), 
        // nếu không có, bạn có thể gửi '' (tùy theo yêu cầu). Nhưng format phải chuẩn nếu gửi.
        const vnp_TransactionDate = vnp_PayDate || ''; // e.g. '20250906174749' (yyyymmddHHMMSS)
        // vnp_CreateDate là thời điểm request này được sinh (GMT+7), format yyyyMMddHHmmss
        const vnp_CreateDate = new Date().toISOString().replace(/[-T:.Z]/g, '').slice(0, 14);
        const vnp_IpAddr = req.ip || '127.0.0.1';

        const vnpayHost = 'https://sandbox.vnpayment.vn/merchant_webapi/api/transaction';
        const secretKey = vnpayConfig.secureSecret;

        // IMPORTANT: thứ tự các tham số khi hash phải đúng theo spec
        const dataToHash = [
            vnp_RequestId,
            vnp_Version,
            vnp_Command,
            vnp_TmnCode,
            vnp_TxnRef,
            vnp_TransactionDate,
            vnp_CreateDate,
            vnp_IpAddr,
            vnp_OrderInfo || ''
        ].join('|');

        const vnp_SecureHash = crypto.createHmac('sha512', secretKey).update(dataToHash).digest('hex');

        const payload = {
            vnp_RequestId,
            vnp_Version,
            vnp_Command,
            vnp_TmnCode,
            vnp_TxnRef,
            vnp_TransactionDate,
            vnp_CreateDate,
            vnp_IpAddr,
            vnp_OrderInfo: vnp_OrderInfo || '',
            vnp_SecureHash
        };

        console.log('Payload gửi sang VNPAY (check refund):', payload);

        const response = await axios.post(vnpayHost, payload, {
            headers: { 'Content-Type': 'application/json' }
        });

        console.log('--- Kết quả check refund ---');
        console.log(response.data);
        return res.json(response.data);
    } catch (err) {
        console.error('Lỗi check refund:', err.message, err.response?.data);
        return res.status(500).json({ error: err.message, detail: err.response?.data });
    }
});


app.listen(7000, () => {
    console.log('Server is running at port 7000');
});
