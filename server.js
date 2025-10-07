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
const ORDERS_API_BASE = process.env.ORDERS_API_BASE || 'http://localhost:7700';
async function markOrderPaid(orderRef, method = 'unknown', txnId = null) {
    if (!orderRef) {
        console.warn('markOrderPaid: missing orderRef, skip update');
        return;
    }
    try {
        const body = {
            paymentStatus: 'paid',
            paymentMethod: method,
            orderStatus: 'confirmed',
        };
        if (txnId) {
            if (method === 'momo') body.transId = txnId;
            else if (method === 'zalopay') body.zp_trans_id = txnId;
            else body.paymentReference = txnId;
        }

        // update order status in Orders service
        await axios.put(`${ORDERS_API_BASE}/api/orders/${encodeURIComponent(orderRef)}`, body, { timeout: 5000 });
        console.log(`Order ${orderRef} updated:`, body);

        // fetch updated order to inspect items/metadata
        let order = null;
        try {
            const resp = await axios.get(`${ORDERS_API_BASE}/api/orders/${encodeURIComponent(orderRef)}`, { timeout: 5000 });
            order = resp.data;
        } catch (err) {
            console.warn('markOrderPaid: failed to fetch order details', err.response?.data || err.message);
        }

        // If order contains tour items, call tour-service to reserve slots
        if (order && Array.isArray(order.items) && order.items.length) {
            const TOUR_SERVICE = process.env.TOUR_SERVICE_BASE || 'http://localhost:8080';
            const snapshot = order.metadata?.bookingDataSnapshot || order.metadata || {};
            for (const it of order.items) {
                if (!it || (it.type && String(it.type).toLowerCase() !== 'tour')) continue;
                const tourId = it.productId || it.itemId;
                if (!tourId) continue;

                // derive dateIso and paxCount from snapshot (adjust to your snapshot shape)
                const dateRaw = snapshot.details?.startDateTime ?? snapshot.details?.date ?? snapshot.date;
                const dateIso = dateRaw ? new Date(dateRaw).toISOString().split('T')[0] : null;

                let paxCount = 1;
                if (Array.isArray(snapshot.details?.passengers)) {
                    paxCount = snapshot.details.passengers.length || 1;
                } else if (snapshot.passengers?.counts) {
                    const c = snapshot.passengers.counts;
                    paxCount = Number(c.adults || 0) + Number(c.children || 0) + Number(c.infants || 0) || 1;
                } else {
                    // fallback to order.items quantity if meaningful
                    paxCount = Number(it.quantity || 1) || 1;
                }

                if (!dateIso) {
                    console.warn('markOrderPaid: missing dateIso for tour item', { tourId, orderRef });
                    continue;
                }

                try {
                    await axios.post(`${TOUR_SERVICE}/api/tours/slots/reserve`, { tourId, dateIso, paxCount }, { timeout: 5000 });
                    console.log(`Reserved ${paxCount} pax for tour ${tourId} on ${dateIso}`);
                } catch (err) {
                    console.error(`Failed to reserve slot for tour ${tourId} ${dateIso}:`, err.response?.data || err.message);
                    // consider retry or compensating logic
                }
            }
        }
    } catch (err) {
        console.error(`Failed to update order ${orderRef} on ${ORDERS_API_BASE}:`, err.response?.data || err.message);
    }
}
// ========== MoMo API ==========
app.post('/momo/payment', async (req, res) => {
    console.log('[MoMo] Request body:', req.body);
    try {
        // lấy từ client nếu có, fallback về config/test
        const {
            amount: amtFromClient,
            orderInfo: orderInfoFromClient,
            partnerCode: partnerCodeFromClient,
            redirectUrl: redirectUrlFromClient,
            ipnUrl: ipnUrlFromClient,
            extraData: extraDataFromClient,
            requestType: requestTypeFromClient,
            partnerCode: partnerCodeCfg,
            accessKey: accessKeyCfg,
            secretKey: secretKeyCfg,
            lang: langCfg,
            autoCapture: autoCaptureCfg,
        } = { ...momoConfig, ...req.body };

        // const amount = String(amtFromClient || req.body.amount || '10000');
        const amount =  '10000'

        const partnerCode = partnerCodeFromClient || partnerCodeCfg || momoConfig.partnerCode;
        const accessKey = accessKeyCfg || momoConfig.accessKey;
        const secretKey = secretKeyCfg || momoConfig.secretKey;
        const orderInfo = orderInfoFromClient || 'Thanh toán';
        const redirectUrl = redirectUrlFromClient || momoConfig.redirectUrl;
        const ipnUrl = ipnUrlFromClient || momoConfig.ipnUrl || 'http://localhost:3000/thanh-toan-thanh-cong';
        const extraData = extraDataFromClient || '';
        const requestType = requestTypeFromClient || requestTypeFromClient || momoConfig.requestType || 'payWithMethod';
        const requestId = (req.body.requestId) ? String(req.body.requestId) : `${partnerCode}${Date.now()}`;
        const orderId = req.body.orderId || `${partnerCode}${Date.now()}`;

        const rawSignature =
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

        const signature = crypto.createHmac('sha256', secretKey).update(rawSignature).digest('hex');

        const requestBody = JSON.stringify({
            partnerCode,
            partnerName: 'MegaTrip',
            storeId: 'MegaTripStore',
            requestId,
            amount,
            orderId,
            orderInfo,
            redirectUrl,
            ipnUrl,
            lang: langCfg || 'vi',
            requestType,
            autoCapture: autoCaptureCfg,
            extraData,
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
    try {
        const body = req.body || {};
        // common MoMo success indicators: resultCode === 0 or errCode === 0 depending on API
        const success = (typeof body.resultCode !== 'undefined' && Number(body.resultCode) === 0)
            || (typeof body.errorCode !== 'undefined' && Number(body.errorCode) === 0)
            || (body.status && String(body.status).toLowerCase() === 'success');

        // order id we passed earlier as orderId when creating payment
        const orderRef = body.orderId || body.orderid || body.requestId || body.orderInfo || body.extraData || null;
        const txnId = body.transId || body.transactionId || body.requestId || null;

        if (success && orderRef) {
            // update order in orders service
            await markOrderPaid(orderRef, 'momo', txnId);
        } else {
            console.log('[MoMo] callback not-success or missing orderRef:', { success, orderRef, body });
        }

        // respond quickly to MoMo
        return res.status(204).end();
    } catch (error) {
        console.log('[MoMo] /callback error:', error.message);
        return res.status(500).json({ statusCode: 500, message: error.message });
    }
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
    try {
        const {
            amount = 50000,
            description = 'Thanh toán MegaTrip',
            app_user = 'user123',
            callback_url = 'https://334f29e30799.ngrok-free.app/zalo/callback',
            embed_data = {},
            items = [],
            // FE sends orderId (createdOrder.orderNumber or _id)
            orderId
        } = req.body;

        // ensure embed_data contains internal order ref
        if (orderId) embed_data.orderNumber = embed_data.orderNumber || orderId;

        // Derive app_trans_id for Zalo:
        // - if orderId starts with 'ORD_' strip that prefix for Zalo
        // - try to form YYMMDD_suffix when possible; otherwise generate YYMMDD_random
        let provided = null;
        if (orderId) provided = String(orderId).startsWith('ORD_') ? String(orderId).replace(/^ORD_/, '') : String(orderId);

        const appTransRegex = /^[0-9]{6}_[0-9A-Za-z]+$/;
        let app_trans_id = null;

        if (provided && appTransRegex.test(String(provided))) {
            app_trans_id = String(provided);
        } else if (provided) {
            const m = String(provided).match(/^(\d{4})(\d{2})(\d{2})[-_]?(.+)$/);
            if (m) {
                const yyMMdd = `${m[1].slice(2)}${m[2]}${m[3]}`;
                const suffix = m[4].replace(/[^0-9A-Za-z]/g, '').slice(0, 32) || Math.floor(Math.random() * 900000 + 100000);
                app_trans_id = `${yyMMdd}_${suffix}`;
            }
        }

        if (!app_trans_id) {
            const rnd = Math.floor(Math.random() * 900000) + 100000;
            app_trans_id = `${moment().format('YYMMDD')}_${rnd}`;
        }

        const order = {
            app_id: zaloConfig.app_id,
            app_trans_id,
            app_user,
            app_time: Date.now(),
            item: JSON.stringify(items || []),
            embed_data: JSON.stringify(embed_data || { redirecturl: callback_url }),
            amount: Number(amount),
            callback_url,
            description,
        };

        const dataStr =
            zaloConfig.app_id + '|' + order.app_trans_id + '|' + order.app_user + '|' + order.amount + '|' + order.app_time + '|' + order.embed_data + '|' + order.item;
        order.mac = CryptoJS.HmacSHA256(dataStr, zaloConfig.key1).toString();

        console.log('[ZaloPay] PAYMENT DEBUG', { order, dataStr, embed_data, originalOrderId: orderId });
        const result = await axios.post(zaloConfig.endpoint, null, { params: order });
        console.log('[ZaloPay] Payment response:', result.data);
        return res.status(200).json(result.data);
    } catch (error) {
        console.log('[ZaloPay] Payment error:', error.response ? error.response.data : error.message);
        return res.status(500).json({ error: error.message });
    }
});

app.post('/zalo/callback', async (req, res) => {
    console.log('[ZaloPay] CALLBACK RECEIVED - headers:', req.headers);
    console.log('[ZaloPay] CALLBACK RECEIVED - raw body:', req.body);

    let result = {};
    try {
        const dataStr = req.body.data || req.body.dataStr || req.body.payload || null;
        const reqMac = req.body.mac || req.body.sign || null;

        console.log('[ZaloPay] dataStr found:', !!dataStr, 'mac found:', !!reqMac);

        if (dataStr && reqMac) {
            const computedMac = CryptoJS.HmacSHA256(dataStr, zaloConfig.key2).toString();
            console.log('[ZaloPay] computed mac:', computedMac);
            if (computedMac !== reqMac) {
                console.warn('[ZaloPay] MAC mismatch - callback rejected');
                result.return_code = -1;
                result.return_message = 'mac not equal';
                return res.json(result);
            }
        }

        let dataJson = null;
        if (dataStr) {
            try { dataJson = typeof dataStr === 'string' ? JSON.parse(dataStr) : dataStr; }
            catch (e) { dataJson = { raw: dataStr }; }
        } else {
            dataJson = { ...req.body };
        }

        console.log('[ZaloPay] parsed callback data:', dataJson);

        const appTransId = dataJson.app_trans_id || dataJson.appTransId || dataJson.app_trans || null;
        const zpTransId = dataJson.zp_trans_id || dataJson.zpTransId || dataJson.zp_trans || null;
        const returnCode = Number(dataJson.return_code ?? dataJson.returnCode ?? dataJson.rc ?? 0);
        const serverTime = dataJson.server_time ?? dataJson.serverTime ?? null;

        // parse embed_data if present (may be a JSON string)
        let embeddedOrderNumber = null;
        if (dataJson.embed_data) {
            try {
                const ed = typeof dataJson.embed_data === 'string' ? JSON.parse(dataJson.embed_data) : dataJson.embed_data;
                embeddedOrderNumber = ed.orderNumber || ed.orderId || null;
            } catch (e) { /* ignore */ }
        }

        console.log('[ZaloPay] extracted => appTransId:', appTransId, 'zpTransId:', zpTransId, 'return_code:', returnCode, 'server_time:', serverTime, 'embed_order:', embeddedOrderNumber);

        // Determine success: treat zp_trans_id or server_time + appTransId as success
        const success = (returnCode === 1) || (!!zpTransId && !!appTransId) || (!!serverTime && !!appTransId);

        // Resolve order reference:
        // prefer embed_data.orderNumber; else re-add ORD_ prefix to appTransId
        let orderRef = embeddedOrderNumber || null;
        if (!orderRef && appTransId) {
            const s = String(appTransId);
            orderRef = s.startsWith('ORD_') ? s : `ORD_${s}`;
        }

        console.log('[ZaloPay] resolved orderRef:', orderRef);

        if (success && orderRef) {
            try {
                await markOrderPaid(orderRef, 'zalopay', String(zpTransId || ''));
                console.log(`[ZaloPay] markOrderPaid called for ${orderRef} zp_trans_id=${zpTransId}`);
            } catch (e) {
                console.error('[ZaloPay] markOrderPaid error:', e?.message || e);
            }
        } else {
            console.log('[ZaloPay] callback not-success or missing orderRef:', { returnCode, appTransId, zpTransId, serverTime, embeddedOrderNumber });
        }

        // acknowledge so gateway won't retry
        result.return_code = 1;
        result.return_message = 'success';
    } catch (ex) {
        console.error('[ZaloPay] callback handler error:', ex);
        result.return_code = 0;
        result.return_message = String(ex?.message || ex);
    }
    return res.json(result);
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
    try {
        const vnpay = new VNPay({ ...vnpayConfig, loggerFn: ignoreLogger });

        const {
            amount = 50000,
            orderInfo = 'Thanh toan MegaTrip',
            returnUrl = 'http://localhost:3002/vnpay/check-payment',
            locale = VnpLocale.VN,
            orderType = ProductCode.Other,
            expireDays = 1,
            txnRef // optional provided by client
        } = req.body || {};

        const txnRefVal = txnRef || `${Date.now()}${Math.floor(Math.random() * 1000)}`;
        const tomorrow = new Date();
        tomorrow.setDate(tomorrow.getDate() + (Number(expireDays) || 1));

        const vnpayResponse = await vnpay.buildPaymentUrl({
            vnp_Amount: Number(amount),
            vnp_IpAddr: req.ip || '127.0.0.1',
            vnp_TxnRef: txnRefVal,
            vnp_OrderInfo: orderInfo,
            vnp_OrderType: orderType,
            vnp_ReturnUrl: returnUrl,
            vnp_Locale: locale,
            vnp_CreateDate: dateFormat(new Date()),
            vnp_ExpireDate: dateFormat(tomorrow),
        });

        console.log('VNPay paymentUrl created:', vnpayResponse);
        return res.status(201).json({ paymentUrl: vnpayResponse, vnp_TxnRef: txnRefVal });
    } catch (err) {
        console.error('VNPay create_payment_url error:', err);
        return res.status(500).json({ error: err.message });
    }
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
    console.log('Server Payment is running at port 7000');
});
