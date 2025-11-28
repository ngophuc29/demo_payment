// Tổng hợp API MoMo, ZaloPay, VNPay
const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const axios = require('axios');
const crypto = require('crypto');
const moment = require('moment');
const cron = require('node-cron');
const qs = require('qs');
const CryptoJS = require('crypto-js');
const { VNPay, ignoreLogger, ProductCode, VnpLocale, dateFormat } = require('vnpay');

const momoConfig = require('./momo/config');

const app = express();
app.use(cors());
app.use(express.json());
app.use(bodyParser.urlencoded({ extended: true }));
const ORDERS_API_BASE = process.env.ORDERS_API_BASE || 'http://localhost:7700';
const TOUR_SERVICE = process.env.TOUR_SERVICE_BASE || 'http://localhost:7700';
const BUS_SERVICE = process.env.BUS_SERVICE_BASE || ORDERS_API_BASE || 'http://localhost:7700'; // bus endpoints live on orders app in local setup
const AUTH_SERVICE = process.env.AUTH_SERVICE || 'http://localhost:7700';
// ...existing code...
function toDateIso(v) {
    try { return (new Date(v)).toISOString().split('T')[0]; } catch { return null; }
}
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

async function issueTicketsForOrder(order) {
    if (!order) return [];
    const ORD = process.env.ORDERS_API_BASE || 'http://localhost:7700';
    const snapshot = order.metadata?.bookingDataSnapshot || order.metadata || {};
    const created = [];

    // helper to get string _id
    const orderIdStr = (() => {
        if (!order._id) return null;
        if (typeof order._id === 'string') return order._id;
        if (order._id.$oid) return order._id.$oid;
        return String(order._id);
    })();

    const postTicket = async (payload) => {
        try {
            const resp = await axios.post(`${ORD}/api/tickets`, payload, { timeout: 8000 });
            if (resp && resp.data && resp.data.ticket) return resp.data.ticket;
            if (resp && resp.data && resp.data.ok && resp.data.ticket) return resp.data.ticket;
            return resp.data || resp;
        } catch (e) {
            console.error('issueTicketsForOrder: create ticket failed', e.response?.data || e.message || e);
            throw e;
        }
    };

    for (const it of order.items || []) {
        const type = String(it.type || '').toLowerCase();
        const productId = it.productId || it.itemId || null;
        if (!productId) continue;

        if (type === 'bus') {
            const details = snapshot.details || {};
            const seats = Array.isArray(details.seats) ? details.seats : [];
            const paxArr = Array.isArray(details.passengers) ? details.passengers : (details.passengerInfo ? [details.passengerInfo] : []);
            const travelIso = snapshot.meta?.departureDateIso || details.date || snapshot.date;
            const travelDate = travelIso ? new Date(travelIso).toISOString().split('T')[0] : null;  // Fix: travelIso is now YYYY-MM-DD, so this works correctly
            const pricing = snapshot.pricing || {};
            const perPax = pricing.perPax || {};
            for (let i = 0; i < (paxArr.length || Number(it.quantity || 1)); i++) {
                const p = paxArr[i] || {};
                const seat = seats[i] || null;
                const paxType = p.type || (i === 0 ? 'adult' : 'adult');
                let price = 0;
                if (paxType === 'child') price = Number(perPax.childUnit || 0);
                else if (paxType === 'infant') price = Number(perPax.infantUnit || 0);
                else price = Number(perPax.adultUnit || 0);
                if (!price) price = Math.round(Number(order.total || it.unitPrice || 0) / Math.max(1, paxArr.length || Number(it.quantity || 1)));

                const passengerName = p.name || [p.title, p.firstName, p.lastName].filter(Boolean).join(' ').trim() || p.firstName || p.lastName || `Pax ${i + 1}`;
                const uniq = `${order.orderNumber || orderIdStr}::bus::${productId}::${travelDate || ''}::seat:${seat ? seat : 'paxIndex:' + i}`;

                const payload = {
                    orderId: orderIdStr,
                    orderNumber: order.orderNumber || null,
                    type: 'bus',
                    productId,
                    passengerIndex: i,
                    passenger: { name: passengerName, type: paxType, idNumber: p.idNumber || '', dob: p.dateOfBirth || '' },
                    seats: seat ? [seat] : [],
                    travelDate: travelDate,
                    travelStart: travelIso && details.time ? `${travelIso}T${details.time.split(' - ')[0]}:00` : null,  // Fix: Use local date + local time
                    travelEnd: null,
                    price,
                    currency: snapshot.meta?.currency || 'VND',
                    reservationInfo: snapshot,
                    uniq
                };

                try {
                    const ticket = await postTicket(payload);
                    created.push(ticket);
                } catch (e) {
                    // log and continue to next passenger
                    console.error('issueTicketsForOrder: failed to create bus ticket for passenger', i, e.message || e);
                }
            }
        } else if (type === 'tour') {
            const details = snapshot.details || {};
            const paxArr = Array.isArray(details.passengers) ? details.passengers : [];
            const travelIso = details.startDateTime || details.date || snapshot.meta?.startDateTime || null;
            // Fix: travelIso is now local YYYY-MM-DD, so use it directly
            const travelDate = travelIso;  // Local YYYY-MM-DD
            const pricing = snapshot.pricing || {};
            const perPax = pricing.perPax || {};
            const tourCode = details.tourCode || productId;

            for (let i = 0; i < (paxArr.length || Number(it.quantity || 1)); i++) {
                const p = paxArr[i] || {};
                const paxType = p.type || 'adult';
                let price = 0;
                if (paxType === 'child') price = Number(perPax.childUnit || 0);
                else if (paxType === 'infant') price = Number(perPax.infantUnit || 0);
                else price = Number(perPax.adultUnit || 0);
                if (!price) price = Math.round(Number(order.total || it.unitPrice || 0) / Math.max(1, paxArr.length || Number(it.quantity || 1)));

                const passengerName = p.name || [p.title, p.firstName, p.lastName].filter(Boolean).join(' ').trim() || p.firstName || p.lastName || `Pax ${i + 1}`;
                const uniq = `${order.orderNumber || orderIdStr}::tour::${tourCode}::${travelDate || ''}::paxIndex:${i}`;

                const payload = {
                    orderId: orderIdStr,
                    orderNumber: order.orderNumber || null,
                    type: 'tour',
                    productId: tourCode,
                    passengerIndex: i,
                    passenger: { name: passengerName, type: paxType, idNumber: p.idNumber || '', dob: p.dateOfBirth || '' },
                    seats: [],
                    travelDate: travelDate,  // Local YYYY-MM-DD
                    travelStart: travelIso || null,  // Local YYYY-MM-DD
                    travelEnd: details.endDateTime || null,  // Local YYYY-MM-DD
                    price,
                    currency: snapshot.meta?.currency || 'VND',
                    reservationInfo: snapshot,
                    uniq
                };

                try {
                    const ticket = await postTicket(payload);
                    created.push(ticket);
                } catch (e) {
                    console.error('issueTicketsForOrder: failed to create tour ticket for passenger', i, e.message || e);
                }
            }
        }

        else if (type === 'flight') {
            // New flight logic
            const flights = snapshot.flights || {};
            const outbound = flights.outbound || {};
            const inbound = flights.inbound || {};
            const passengers = Array.isArray(snapshot.details?.passengers) ? snapshot.details.passengers : [];
            const seats = Array.isArray(snapshot.pricing?.seats) ? snapshot.pricing.seats : [];
            const pricing = snapshot.pricing || {};
            const perPax = pricing.perPax || {};

            // Helper to parse date
            const parseFlightDate = (dateStr) => {
                if (!dateStr) return null;
                // Handle formats like "15/01/2025" or "2025-10-17"
                if (dateStr.includes('/')) {
                    const [day, month, year] = dateStr.split('/');
                    return `${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}`;
                }
                return dateStr; // Assume ISO
            };

            // Group seats by leg
            const outboundSeats = seats.filter(s => s.leg === 'outbound').map(s => s.id || s.number);
            const inboundSeats = seats.filter(s => s.leg === 'inbound').map(s => s.id || s.number);

            for (let i = 0; i < passengers.length; i++) {
                const p = passengers[i] || {};
                const paxType = p.type || 'adult';
                let price = 0;
                if (paxType === 'child') price = Number(perPax.childUnit || 0);
                else if (paxType === 'infant') price = Number(perPax.infantUnit || 0);
                else price = Number(perPax.adultUnit || 0);
                if (!price) price = Math.round(Number(order.total || it.unitPrice || 0) / Math.max(1, passengers.length));

                const passengerName = p.name || [p.title, p.firstName, p.lastName].filter(Boolean).join(' ').trim() || p.firstName || p.lastName || `Pax ${i + 1}`;

                // Outbound ticket
                if (outbound.flightNumber) {
                    const travelDate = parseFlightDate(outbound.date);
                    const seat = outboundSeats[i] || null;
                    const uniq = `${order.orderNumber || orderIdStr}::flight::${outbound.flightNumber}::outbound::paxIndex:${i}`;
                    const payload = {
                        orderId: orderIdStr,
                        orderNumber: order.orderNumber || null,
                        type: 'flight',
                        productId: `${outbound.flightNumber}__${outbound.route}__${outbound.date}__${outbound.time}` || outbound.flightNumber,
                        passengerIndex: i,
                        passenger: { name: passengerName, type: paxType, idNumber: p.idNumber || '', dob: p.dateOfBirth || '' },
                        seats: seat ? [seat] : [],
                        travelDate: travelDate,
                        travelStart: outbound.time ? `${travelDate}T${outbound.time.split(' - ')[0]}:00` : null,
                        travelEnd: outbound.time ? `${travelDate}T${outbound.time.split(' - ')[1]}:00` : null,
                        price,
                        currency: snapshot.meta?.currency || 'VND',
                        reservationInfo: snapshot,
                        uniq
                    };

                    try {
                        const ticket = await postTicket(payload);
                        created.push(ticket);
                    } catch (e) {
                        console.error('issueTicketsForOrder: failed to create flight outbound ticket for passenger', i, e.message || e);
                    }
                }

                // Inbound ticket
                if (inbound.flightNumber) {
                    const travelDate = parseFlightDate(inbound.date);
                    const seat = inboundSeats[i] || null;
                    const uniq = `${order.orderNumber || orderIdStr}::flight::${inbound.flightNumber}::inbound::paxIndex:${i}`;
                    const payload = {
                        orderId: orderIdStr,
                        orderNumber: order.orderNumber || null,
                        type: 'flight',
                        productId: `${inbound.flightNumber}_${inbound.route}_${inbound.date}_${inbound.time}` || inbound.flightNumber,
                        passengerIndex: i,
                        passenger: { name: passengerName, type: paxType, idNumber: p.idNumber || '', dob: p.dateOfBirth || '' },
                        seats: seat ? [seat] : [],
                        travelDate: travelDate,
                        travelStart: inbound.time ? `${travelDate}T${inbound.time.split(' - ')[0]}:00` : null,
                        travelEnd: inbound.time ? `${travelDate}T${inbound.time.split(' - ')[1]}:00` : null,
                        price,
                        currency: snapshot.meta?.currency || 'VND',
                        reservationInfo: snapshot,
                        uniq
                    };

                    try {
                        const ticket = await postTicket(payload);
                        created.push(ticket);
                    } catch (e) {
                        console.error('issueTicketsForOrder: failed to create flight inbound ticket for passenger', i, e.message || e);
                    }
                }
            }
        } else {
            // other product types: skip
            continue;
        }
    } // end items loop

    console.log('issueTicketsForOrder: created tickets count', created.length);
    return created;
}
const TICKET_SERVICE_BASE = process.env.TICKET_SERVICE_BASE || 'http://localhost:7700';

// xử cho đổi vé
async function handleChangeCalendarPayment(originalOrder, method, txnId) {
    try {
        // Lấy thông tin từ inforChangeCalendar của order gốc
        const changeCalendarData = originalOrder.inforChangeCalendar || {};
        const changeDate = changeCalendarData.data?.changeDate || new Date().toISOString().split('T')[0];

        console.log('changeCalendarData.data:', changeCalendarData.data); // Log để debug
        let newProductId = null;
        let newFlights = null;
        // 1. Cập nhật inforChangeCalendar trong order gốc qua API
        const updatePayload = {
            changeCalendar: true,
            dateChangeCalendar: changeDate,
            inforChangeCalendar: {
                ...changeCalendarData,
                status: 'paid',
                ...(method === 'momo' && { transId: txnId }),
                ...(method === 'zalopay' && { zp_trans_id: txnId })
            },
            ticketIds: [], // Sẽ update sau
            oldTicketIDs: originalOrder.ticketIds || [] // Lưu vé cũ
        };

        // Update order gốc qua API (thay vì save() trên plain object)
        await axios.put(`${ORDERS_API_BASE}/api/orders/${encodeURIComponent(originalOrder._id || originalOrder.orderNumber)}`, updatePayload, { timeout: 5000 });

        // 2. Cập nhật metadata, serviceDate, và items cho flight (không cần release/reserve slots)
        const flightItem = originalOrder.items.find(it => it.type === 'flight');
        
        if (flightItem) {
            // Sửa: Lấy selectedOption từ meta thay vì trực tiếp từ data
            const newTime = changeCalendarData.data?.meta?.newTime || '00:00 - 00:00';
            const selectedOption = changeCalendarData.data?.meta?.selectedOption || changeCalendarData.data?.selectedOption || {};
            const selectedSeats = changeCalendarData.data?.meta?.selectedSeats || []; // Sửa: Lấy từ meta.selectedSeats
            const newDate = changeCalendarData.data?.newDate || changeCalendarData.data?.changeDate || changeDate;

            console.log('selectedOption:', selectedOption); // Log để debug
            console.log('newDate:', newDate, 'changeDate:', changeDate); // Log để debug

            // Tạo productId mới dựa trên selectedOption, dùng changeDate cho date để đảm bảo đúng ngày đổi
            // Tạo newProductId match format vé cũ, dùng newTime từ meta
            newProductId = selectedOption.flightNumber
                ? `${selectedOption.flightNumber}__${selectedOption.departure?.airport || 'DEP'} → ${selectedOption.arrival?.airport || 'ARR'}__${changeDate}__${newTime}`
                : flightItem.productId;

            console.log('newProductId calculated:', newProductId); // Log để debug

            // Tính unitPrice mới từ selectedOption.price
            const newUnitPrice = selectedOption.price || flightItem.unitPrice || 0;

            // Tạo newFlights object với outbound (để match logic cũ như mapOrderToBooking expect flights.outbound.date), dùng newDate cho date
            newFlights = {
                outbound: {
                    id: selectedOption.raw?.id || selectedOption.id,
                    flightNumber: selectedOption.flightNumber,
                    airline: selectedOption.airlineCode || selectedOption.airline,
                    route: `${selectedOption.departure?.airport || ''} → ${selectedOption.arrival?.airport || ''}`,
                    date: newDate,  // Dùng newDate thay vì selectedOption.departure?.date || newDate để đảm bảo đúng ngày mới
                    time: selectedOption.time || `${selectedOption.departure?.time || ''} - ${selectedOption.arrival?.time || ''}`,
                    itineraries: selectedOption.raw?.itineraries || [],
                    currency: selectedOption.currency || 'VND'
                },
                inbound: null // Giả sử one-way
            };

            const updateData = {
                serviceDate: newDate,
                'metadata.flights': newFlights,  // Sử dụng newFlights object
                'metadata.pricing.seats': selectedSeats,
                // Cập nhật snapshot để issueTicketsForOrder dùng data mới
                'metadata.bookingDataSnapshot.pricing.seats': selectedSeats,
                'metadata.bookingDataSnapshot.flights': newFlights,  // Sử dụng newFlights object
                // Update items[0]
                'items.0.productId': newProductId,
                'items.0.name': `${selectedOption.airline || 'Unknown'} ${selectedOption.flightNumber || ''} - ${selectedOption.departure?.city || ''} to ${selectedOption.arrival?.city || ''}`.trim() || flightItem.name,
                'items.0.unitPrice': newUnitPrice,
                'items.0.subtotal': newUnitPrice * (flightItem.quantity || 1),
            };

            await axios.put(`${ORDERS_API_BASE}/api/orders/${encodeURIComponent(originalOrder._id || originalOrder.orderNumber)}`, updateData, { timeout: 5000 });
        }

        // Định nghĩa selectedSeats ở đây để dùng cho tất cả types (flight/bus), đảm bảo luôn là array từ meta
        const selectedSeats = changeCalendarData.data?.meta?.selectedSeats || []; // Sửa: Lấy từ meta.selectedSeats
        console.log('selectedSeats defined:', selectedSeats); // Log để debug



        // 2. Release slots cho ngày cũ và reserve cho ngày mới (cho Tour và Bus)
        const snapshot = originalOrder.metadata?.bookingDataSnapshot || {};
        for (const it of originalOrder.items || []) {
            const productId = it.productId || it.itemId;
            if (!productId) continue;

            // Ngày cũ: từ snapshot
            const oldDateRaw = snapshot.details?.startDateTime ?? snapshot.details?.date;
            const oldDateIso = toDateIso(oldDateRaw);

            // Ngày mới: từ changeDate
            const newDateIso = changeDate;

            if (it.type === 'tour') {
                // Xử lý Tour (như cũ)
                const { seatCount } = seatConsumingCounts(snapshot, it);
                const paxCount = seatCount;
                if (oldDateIso) {
                    try {
                        await releaseViaHttp(productId, oldDateIso, null, originalOrder.orderNumber);
                        console.log(`Released ${paxCount} pax for tour ${productId} on old date ${oldDateIso} (change calendar)`);
                    } catch (e) {
                        console.error('Release tour failed for old date:', e.message);
                    }
                }
                if (newDateIso) {
                    try {
                        await reserveViaHttp(productId, newDateIso, paxCount, null, originalOrder.orderNumber);
                        console.log(`Reserved ${paxCount} pax for tour ${productId} on new date ${newDateIso} (change calendar)`);
                    } catch (e) {
                        console.error('Reserve tour failed for new date:', e.message);
                    }
                }
            } else if (it.type === 'bus') {
                // Xử lý Bus (với seats)
                // Seats cũ: từ ticketIds hiện tại
                let oldSeats = [];
                try {
                    const ticketIds = originalOrder.ticketIds || [];
                    for (const tid of ticketIds) {
                        const ticketResp = await axios.get(`${TICKET_SERVICE_BASE}/api/tickets/${encodeURIComponent(tid)}`, { timeout: 5000 });
                        const ticket = ticketResp.data?.ticket || ticketResp.data;
                        if (ticket && Array.isArray(ticket.seats)) {
                            oldSeats.push(...ticket.seats);
                        }
                    }
                } catch (e) {
                    console.error('Error fetching old seats for bus:', e.message);
                }

                // Seats mới: từ meta (lưu từ bước 2)
                let newSeats = changeCalendarData.data?.meta?.selectedSeats || []; // Sửa: Lấy từ meta.selectedSeats
                try {
                    const meta = originalOrder.inforChangeCalendar?.data?.meta;
                    if (meta && Array.isArray(meta.selectedSeats)) {
                        newSeats = meta.selectedSeats;
                    }
                } catch (e) {
                    console.error('Error parsing new seats for bus:', e.message);
                }

                // Release ngày cũ
                if (oldDateIso && oldSeats.length > 0) {
                    try {
                        await axios.post(`${BUS_SERVICE}/api/buses/${encodeURIComponent(productId)}/slots/release`, {
                            dateIso: oldDateIso,
                            seats: oldSeats,
                            reservationId: originalOrder.orderNumber,
                            orderNumber: originalOrder.orderNumber
                        }, { timeout: 5000 });
                        console.log(`Released seats ${oldSeats.join(', ')} for bus ${productId} on old date ${oldDateIso} (change calendar)`);
                    } catch (e) {
                        console.error('Release bus failed for old date:', e.message);
                    }
                }

                // Reserve ngày mới
                if (newDateIso) {
                    try {
                        const reserveBody = {
                            dateIso: newDateIso,
                            reservationId: originalOrder.orderNumber,
                            orderNumber: originalOrder.orderNumber
                        };
                        if (newSeats.length > 0) {
                            reserveBody.seats = newSeats;
                        } else {
                            // Fallback: reserve theo count nếu không có seats
                            const { seatCount } = seatConsumingCounts(snapshot, it);
                            reserveBody.count = seatCount;
                        }
                        await axios.post(`${BUS_SERVICE}/api/buses/${encodeURIComponent(productId)}/slots/reserve`, reserveBody, { timeout: 5000 });
                        console.log(`Reserved seats ${newSeats.join(', ') || `count: ${reserveBody.count}`} for bus ${productId} on new date ${newDateIso} (change calendar)`);
                    } catch (e) {
                        console.error('Reserve bus failed for new date:', e.message);
                    }
                }
            }
            // Không thêm else if cho flight, vì không cần slots
        }

        // 3. Lấy vé cũ từ oldTicketIDs hoặc ticketIds và xử lý qua API
        const oldTicketIds = originalOrder.ticketIds || [];
        const newTickets = [];

        // Lấy newSeats từ meta (cho bus)
        let newSeats = changeCalendarData.data?.meta?.selectedSeats || []; // Sửa: Lấy từ meta.selectedSeats
        try {
            const meta = originalOrder.inforChangeCalendar?.data?.meta;
            if (meta && Array.isArray(meta.selectedSeats)) {
                newSeats = meta.selectedSeats;
            }
        } catch (e) {
            console.error('Error parsing new seats for bus:', e.message);
        }

        for (const ticketId of oldTicketIds) {
            try {
                // Lấy thông tin vé cũ qua API
                const oldTicketResp = await axios.get(`${TICKET_SERVICE_BASE}/api/tickets/${encodeURIComponent(ticketId)}`, { timeout: 5000 });
                const oldTicket = oldTicketResp.data?.ticket || oldTicketResp.data;
                if (!oldTicket) continue;

                // Cập nhật status vé cũ thành 'cancelled' qua API
                await axios.patch(`${TICKET_SERVICE_BASE}/api/tickets/${encodeURIComponent(ticketId)}/status`, {
                    status: 'cancelled',
                    reason: 'Đổi lịch - vé cũ bị hủy',
                    by: { type: 'system', reason: 'change_calendar' }
                }, { timeout: 5000 });

                // Tạo vé mới qua API (dùng changeDate từ inforChangeCalendar)
                const newTicketPayload = {
                    ticketNumber: `TKT_${originalOrder.orderNumber}_${Date.now()}_${Math.floor(Math.random() * 1000)}`,
                    orderId: originalOrder._id,
                    orderNumber: originalOrder.orderNumber,
                    type: oldTicket.type,
                    productId: newProductId || oldTicket.productId,  // Dùng newProductId nếu có (cho flight), else old
                    passengerIndex: oldTicket.passengerIndex,
                    passenger: oldTicket.passenger,
                    seats: [],  // Sẽ update bên dưới
                    travelDate: changeDate, // Dùng changeDate từ inforChangeCalendar
                    travelStart: oldTicket.travelStart,
                    travelEnd: oldTicket.travelEnd,
                    price: oldTicket.price,
                    currency: oldTicket.currency,
                    reservationInfo: oldTicket.reservationInfo,
                    status: 'changed', // Status mới
                    ticketType: oldTicket.ticketType,
                    uniq: `${originalOrder.orderNumber}::changed::${Date.now()}_${oldTicket.passengerIndex}`  // Thêm passengerIndex để uniq khác nhau
                };

                console.log('newTicketPayload.productId before assignment:', newTicketPayload.productId); // Log để debug
                // Nếu là bus, gán seats mới từ newSeats
                if (oldTicket.type === 'bus') {
                    const passengerObj = typeof oldTicket.passenger === 'string' ? JSON.parse(oldTicket.passenger) : oldTicket.passenger;
                    const isInfant = passengerObj?.type === 'infant';
                    if (!isInfant && newSeats[oldTicket.passengerIndex] !== undefined) {
                        newTicketPayload.seats = [newSeats[oldTicket.passengerIndex]];
                    } else {
                        newTicketPayload.seats = [];
                    }
                }

                // Nếu là flight, gán seats mới từ selectedSeats
                if (oldTicket.type === 'flight') {
                    const passengerObj = typeof oldTicket.passenger === 'string' ? JSON.parse(oldTicket.passenger) : oldTicket.passenger;
                    const isInfant = passengerObj?.type === 'infant';
                    console.log('selectedSeats before use:', selectedSeats, 'passengerIndex:', oldTicket.passengerIndex); // Log để debug
                    if (!isInfant && selectedSeats[oldTicket.passengerIndex] !== undefined) {
                        newTicketPayload.seats = [selectedSeats[oldTicket.passengerIndex]];
                    } else {
                        newTicketPayload.seats = [];  // Infant không có seat
                    }

                    // Sync reservationInfo với newFlights để khớp với productId
                    newTicketPayload.reservationInfo = {
                        ...oldTicket.reservationInfo,
                        flights: newFlights  // Dùng newFlights (flight mới) thay vì flight cũ
                    };
                }

                console.log(`Creating new ticket for passengerIndex ${oldTicket.passengerIndex}, productId: ${newTicketPayload.productId}, seats: ${JSON.stringify(newTicketPayload.seats)}, travelDate: ${newTicketPayload.travelDate}`);  // Log chi tiết hơn

                const newTicketResp = await axios.post(`${TICKET_SERVICE_BASE}/api/tickets`, newTicketPayload, { timeout: 8000 });
                const newTicket = newTicketResp.data?.ticket || newTicketResp.data;
                if (newTicket && newTicket._id) {
                    newTickets.push(newTicket._id);
                    console.log(`Successfully created new ticket for passengerIndex ${oldTicket.passengerIndex}: ${newTicket._id}`);  // Log success
                } else {
                    console.error(`Failed to create new ticket for passengerIndex ${oldTicket.passengerIndex}: Invalid response`);  // Log error
                }
            } catch (err) {
                console.error('Error processing ticket via API:', ticketId, err.response?.data || err.message);
                // Tiếp tục với vé khác nếu có lỗi
            }
        }

        // 4. Cập nhật ticketIds trong order gốc qua API
        await axios.patch(`${ORDERS_API_BASE}/api/orders/${encodeURIComponent(originalOrder._id || originalOrder.orderNumber)}`, {
            ticketIds: newTickets,
            oldTicketIDs: oldTicketIds
        }, { timeout: 5000 });

        console.log(`Change calendar payment processed for order ${originalOrder.orderNumber}, new tickets: ${newTickets.length}`);
    } catch (err) {
        console.error('handleChangeCalendarPayment error:', err);
        throw err;
    }
}







// xử lý đơn hàng khi call back ,chuyển status thành paid,...
// find location near end of markOrderPaid where reservations done, then call:
async function markOrderPaid(orderRef, method = 'unknown', txnId = null, extraData = {}) {
    if (!orderRef) {
        console.warn('markOrderPaid: missing orderRef, skip update');
        return;
    }
    try {
        // Kiểm tra nếu là callback cho đơn đổi lịch
        const isChangeCalendar = String(orderRef).startsWith('ORD_FORCHANGE_');
        let originalOrder = null;

        if (isChangeCalendar) {
            // Lấy order gốc từ extraData (MoMo/ZaloPay có originalOrder)
            const originalOrderRef = extraData.originalOrder || extraData.orderNumber;
            if (originalOrderRef) {
                // Lấy order gốc qua API
                const orderResp = await axios.get(`${ORDERS_API_BASE}/api/orders/${encodeURIComponent(originalOrderRef)}`, { timeout: 5000 });
                originalOrder = orderResp.data;
                if (!originalOrder) {
                    console.warn('markOrderPaid: original order not found for change calendar');
                    return;
                }
            } else {
                console.warn('markOrderPaid: no original order reference in extraData');
                return;
            }
        } else {
            // Luồng bình thường: lấy order gốc qua API
            const orderResp = await axios.get(`${ORDERS_API_BASE}/api/orders/${encodeURIComponent(orderRef)}`, { timeout: 5000 });
            originalOrder = orderResp.data;
            if (!originalOrder) {
                console.warn('markOrderPaid: order not found');
                return;
            }
        }

        if (isChangeCalendar) {
            // Logic đặc biệt cho đổi lịch
            await handleChangeCalendarPayment(originalOrder, method, txnId);
        } else {



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
                const TOUR_SERVICE = process.env.TOUR_SERVICE_BASE || 'http://localhost:7700';
                const BUS_SERVICE = process.env.BUS_SERVICE_BASE || ORDERS_API_BASE; // bus endpoints live on orders app in local setup
                const snapshot = order.metadata?.bookingDataSnapshot || order.metadata || {};

                // reserve for tour items
                const tourReservations = [];
                for (const it of order.items) {
                    if (!it || (it.type && String(it.type).toLowerCase() !== 'tour')) continue;
                    const tourId = it.productId || it.itemId;
                    if (!tourId) continue;

                    const dateRaw = snapshot.details?.startDateTime ?? snapshot.details?.date ?? snapshot.date;
                    const dateIso = dateRaw ? new Date(dateRaw).toISOString().split('T')[0] : null;

                    // let paxCount = 1;
                    // if (Array.isArray(snapshot.details?.passengers)) {
                    //     paxCount = snapshot.details.passengers.length || 1;
                    // } else if (snapshot.passengers?.counts) {
                    //     const c = snapshot.passengers.counts;
                    //     paxCount = Number(c.adults || 0) + Number(c.children || 0) + Number(c.infants || 0) || 1;
                    // } else {
                    //     paxCount = Number(it.quantity || 1) || 1;
                    // }
                    // compute seat-consuming pax (adults + children). infants do NOT consume seats.
                    const { seatCount } = seatConsumingCounts(snapshot, it);
                    const paxCount = seatCount;
                    if (!dateIso) {
                        console.warn('markOrderPaid: missing dateIso for tour item', { tourId, orderRef });
                        continue;
                    }

                    const reservationId = order.orderNumber || orderRef;
                    const reqBody = { tourId, dateIso, paxCount, reservationId, orderNumber: order.orderNumber || orderRef, customerId: order.customerId || null };

                    try {
                        const resp = await axios.post(`${TOUR_SERVICE}/api/tours/slots/reserve`, reqBody, { timeout: 5000 });
                        console.log(`Reserved ${paxCount} pax for tour ${tourId} on ${dateIso}`, resp.data);
                        tourReservations.push({ tourId, dateIso, paxCount, reservationId });
                    } catch (err) {
                        console.error(`Failed to reserve slot for tour ${tourId} ${dateIso}:`, err.response?.data || err.message);
                        // rollback previous tour reservations (best-effort)
                        for (const r of tourReservations) {
                            try {
                                await axios.post(`${TOUR_SERVICE}/api/tours/slots/release`, {
                                    tourId: r.tourId,
                                    dateIso: r.dateIso,
                                    reservationId: r.reservationId,
                                    orderNumber: order.orderNumber || orderRef
                                }, { timeout: 5000 });
                                console.log('Rolled back tour reservation', r);
                            } catch (releaseErr) {
                                console.error('Rollback release failed for tour', r, releaseErr.response?.data || releaseErr.message);
                            }
                        }
                        // continue to bus reservation but notify/alert as needed
                        break;
                    }
                }

                // reserve for bus items (idempotent by using reservationId = orderNumber)
                const reservations = []; // track succeeded reservations to rollback on partial failure
                for (const it of order.items) {
                    if (!it || (it.type && String(it.type).toLowerCase() !== 'bus')) continue;
                    const busId = it.productId || it.itemId;
                    if (!busId) continue;

                    // date: prefer meta.departureDateIso then details.date
                    const dateRaw = snapshot.meta?.departureDateIso ?? snapshot.details?.date ?? snapshot.date;
                    // Fix: parse dateRaw to get local Y-M-D string, handling both local strings and UTC ISO strings
                    const dateIso = dateRaw ? (() => {
                        const d = new Date(dateRaw);
                        const year = d.getFullYear();
                        const month = String(d.getMonth() + 1).padStart(2, '0');
                        const day = String(d.getDate()).padStart(2, '0');
                        return `${year}-${month}-${day}`;
                    })() : null;

                    if (!dateIso) {
                        console.warn('markOrderPaid: missing dateIso for bus item', { busId, orderRef });
                        continue;
                    }

                    const seats = Array.isArray(snapshot.details?.seats) && snapshot.details.seats.length ? snapshot.details.seats : null;
                    // let paxCount = 1;
                    // if (Array.isArray(snapshot.details?.passengers)) paxCount = snapshot.details.passengers.length;
                    // else if (snapshot.passengers?.counts) {
                    //     const c = snapshot.passengers.counts;
                    //     paxCount = Number(c.adults || 0) + Number(c.children || 0) + Number(c.infants || 0) || 1;
                    // } else paxCount = Number(it.quantity || 1) || 1;
                    // compute seat-consuming pax (adults + children). infants do NOT consume seats.
                    const { seatCount } = seatConsumingCounts(snapshot, it);
                    const paxCount = seatCount;

                    const reservationId = order.orderNumber || orderRef;
                    const reqBody = { dateIso };
                    // if (seats) reqBody.seats = seats;
                    // else reqBody.count = paxCount;
                    if (seats) reqBody.seats = seats;
                    else reqBody.count = paxCount; // paxCount = adults + children (infants excluded)
                    reqBody.reservationId = reservationId;
                    reqBody.orderNumber = order.orderNumber || orderRef;
                    reqBody.customerId = order.customerId || order.customerId;

                    try {
                        const resp = await axios.post(`${BUS_SERVICE}/api/buses/${encodeURIComponent(busId)}/slots/reserve`, reqBody, { timeout: 5000 });
                        console.log(`Bus reserve success for bus ${busId} on ${dateIso}`, resp.data);
                        reservations.push({ busId, dateIso, seats, paxCount, reservationId });
                    } catch (err) {
                        console.error(`Failed to reserve seats for bus ${busId} on ${dateIso}:`, err.response?.data || err.message);
                        // rollback previous bus reservations created in this loop (best-effort)
                        for (const r of reservations) {
                            try {
                                const body = { dateIso: r.dateIso };
                                // prefer explicit seats array when available
                                if (Array.isArray(r.seats) && r.seats.length) body.seats = r.seats;
                                // fallback: do not rely on count unless bus API supports it; keep as optional
                                else if (Number.isFinite(Number(r.paxCount)) && Number(r.paxCount) > 0) body.count = Number(r.paxCount);
                                // use reservationId / orderNumber from the recorded reservation
                                if (r.reservationId) body.reservationId = r.reservationId;
                                if (r.orderNumber) body.orderNumber = r.orderNumber;

                                await axios.post(`${BUS_SERVICE}/api/buses/${encodeURIComponent(r.busId)}/slots/release`, body, { timeout: 5000 });
                                console.log('Rolled back reservation for bus', { busId: r.busId, dateIso: r.dateIso, reservationId: body.reservationId, seats: body.seats, count: body.count });
                            } catch (releaseErr) {
                                console.error('Rollback release failed for', r, releaseErr.response?.data || releaseErr.message);
                            }
                        }
                        // do not throw to avoid failing markOrderPaid; notify/alert instead
                    }
                } // end bus loop

                try {
                    if (order) {
                        try {
                            const createdTickets = await issueTicketsForOrder(order);
                            console.log('Tickets issued for order', orderRef, 'count=', createdTickets.length);
                        } catch (e) {
                            console.error('markOrderPaid: failed to issue tickets for', orderRef, e?.message || e);
                            // don't throw — we already updated order/payment; just log error for manual retry
                        }
                    }
                } catch (errInner) {
                    // noop: this outer try/catch continues existing behavior
                }
            }
        }



        //gửi mail khi trả về callback thanh toán thành công 
        if (isChangeCalendar) {
            // Gửi mail đổi lịch
            try {
                let extraData = JSON.stringify({
                    originalOrder: originalOrder.orderNumber,
                    changeCode: originalOrder.inforChangeCalendar.codeChange,
                    download: true // Thêm flag download
                });
                await axios.post(`${AUTH_SERVICE}/api/auth/send-change-calendar`, {
                    email: originalOrder.customerEmail,
                    changeDetails: {
                        orderNumber: originalOrder.inforChangeCalendar.codeChange,
                        changeDate: originalOrder.inforChangeCalendar.data.changeDate,
                        fee: originalOrder.inforChangeCalendar.totalpayforChange,
                        note: originalOrder.inforChangeCalendar.data.note,
                        ticketDownloadUrl: `http://localhost:3000/thanh-toan-thanh-cong?orderId=${originalOrder.inforChangeCalendar.codeChange}&extraData=${encodeURIComponent(extraData)}`
                    }
                });
            } catch (e) {
                console.error('Failed to send change calendar email:', e.message);
            }
        } else {
            // Gửi mail đặt 
            try {
                // Xử lý departureDate cho flight
                let departureDate = 'N/A';
                if (originalOrder.items[0]?.type === 'flight') {
                    const flights = originalOrder.metadata?.bookingDataSnapshot?.flights;
                    if (flights?.outbound && flights?.inbound) {
                        departureDate = `${flights.outbound.date} - ${flights.inbound.date}`;
                    } else if (flights?.outbound) {
                        departureDate = flights.outbound.date;
                    } else if (flights?.inbound) {
                        departureDate = flights.inbound.date;
                    }
                } else {
                    departureDate = originalOrder.metadata?.bookingDataSnapshot?.details?.date || 'N/A';
                }

                let extraData = JSON.stringify({
                    originalOrder: originalOrder.orderNumber,
                    download: true // Thêm flag download
                });
                await axios.post(`${AUTH_SERVICE}/api/auth/send-booking-success`, {
                    email: originalOrder.customerEmail,
                    orderDetails: {
                        orderNumber: originalOrder.orderNumber,
                        tourName: originalOrder.items[0]?.name || 'Tour',
                        departureDate: departureDate,
                        total: originalOrder.total,
                        ticketDownloadUrl: `http://localhost:3000/thanh-toan-thanh-cong?orderId=${originalOrder.orderNumber}&extraData=${encodeURIComponent(extraData)}`
                    }
                });
            } catch (e) {
                console.error('Failed to send booking success email:', e.message);
            }
        }


    } // end if order && items
    catch (err) {
        console.error(`Failed to update order ${orderRef} on ${ORDERS_API_BASE}:`, err.response?.data || err.message);
    }
}

app.get('/ping', (req, res) => {
    console.log('Ping received at', new Date().toISOString());
    res.status(200).send('Pong - App is awake');
});
cron.schedule('*/10 * * * *', () => {
    console.log('Internal cron: Pinging self at', new Date().toISOString());
    axios.get('http://localhost:7000/ping').catch(err => console.log('Self-ping failed:', err.message));
});
app.get('/', (req, res) => {
    console.log('Server payment  received at', new Date().toISOString());
    res.status(200).send('Run Successfully');
});
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

        const amount = String(amtFromClient || req.body.amount || '10000');
        // const amount = '10000'

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
            // Parse extraData từ MoMo
            let extraData = {};
            try {
                if (body.extraData) extraData = JSON.parse(body.extraData);
            } catch (e) { }
            await markOrderPaid(orderRef, 'momo', txnId, extraData);
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
            callback_url = 'https://82623b6fe674.ngrok-free.app/zalo/callback',
            embed_data = {},
            items = [],
            redirectUrl,
            orderId
        } = req.body;

        // ensure embed_data contains internal order ref
        if (orderId) embed_data.orderNumber = embed_data.orderNumber || orderId;

        // Validation callback_url: Chỉ chấp nhận localhost:7000 hoặc ngrok URL
        const isValidCallback = callback_url.startsWith('http://localhost:7000') || callback_url.includes('ngrok-free.app');
        const finalCallbackUrl = isValidCallback ? callback_url : 'https://82623b6fe674.ngrok-free.app/zalo/callback';

        // Set redirecturl dùng chung với MoMo và thêm orderId + extraData nếu FOR_CHANGE
        const baseRedirectUrl = redirectUrl || momoConfig.redirectUrl || 'https://your-frontend-domain.com/payment-success';
        const separator = baseRedirectUrl.includes('?') ? '&' : '?';
        let redirectParams = `orderId=${encodeURIComponent(orderId)}`;

        // Nếu là FOR_CHANGE, thêm extraData giống MoMo
        if (orderId && orderId.startsWith('ORD_FORCHANGE_')) {
            const extraData = JSON.stringify({
                originalOrder: embed_data.originalOrder || null,
                changeCode: orderId
            });
            redirectParams += `&extraData=${encodeURIComponent(extraData)}`;
        }

        embed_data.redirecturl = `${baseRedirectUrl}${separator}${redirectParams}`;

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
            embed_data: JSON.stringify(embed_data),  // embed_data giờ bao gồm redirecturl với orderId
            // amount: Number(amount),
            amount: 10000,
            callback_url: finalCallbackUrl,
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

        let embeddedOrderNumber = null;
        let originalOrderFromEmbed = null;
        if (dataJson.embed_data) {
            try {
                const ed = typeof dataJson.embed_data === 'string' ? JSON.parse(dataJson.embed_data) : dataJson.embed_data;
                embeddedOrderNumber = ed.orderNumber || ed.orderId || null;
                originalOrderFromEmbed = ed.originalOrder || null;  // Thêm: Lấy originalOrder từ embed_data
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
                // Sửa: extraData.originalOrder nên là originalOrderFromEmbed (order gốc)
                const extraData = { originalOrder: originalOrderFromEmbed || embeddedOrderNumber };
                await markOrderPaid(orderRef, 'zalopay', zpTransId, extraData);
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
