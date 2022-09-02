#!/usr/bin/env node

import {
    RabbitMQ,
    SqlDB,
    GoogleAdmin,
    AppleAPNs,
    Logger,
    Roles,
    Notification
} from 'ikomida-shared'
import {
    createRequire
} from "module"
const require = createRequire(
    import.meta.url)
let {
    name
} = require("../package.json")
name = name
    .replace(/^(@\S+\/)?(svelte-)?(\S+)/, '$3')
    .replace(/^\w/, m => m.toUpperCase())
    .replace(/-\w/g, m => m[1].toUpperCase())

class VendorPushNotificationWorker {

    googleAdmin
    appleAPNs
    amqp
    logger

    constructor() {
        this.logger = Logger.getInstance(name)
    }

    async run() {
        try {
            this.googleAdmin = new GoogleAdmin(this.logger)
            this.appleAPNs = new AppleAPNs(this.logger)
            this.amqp = new RabbitMQ(this.logger)
            await this.amqp.listenToMessages(RabbitMQ.VENDOR_PUSH_NOTIFICATION_QUEUE, this.processMessages.bind(this))
        } catch (error) {
            this.logger.error(error)
        }
    }

    async processMessages(payload, channel) {
        try {
            this.logger.log(` [x] ${payload.fields.routingKey}: payload received: '${payload.content.toString('utf8')}'`)
            const messageObject = JSON.parse(payload.content.toString('utf8'))
            if (messageObject.method !== 'sendVendorPushNotifications') {
                this.logger.log(` [Error]: O metodo: ${messageObject?.method} não suportado!`)
                channel.ack(payload)
                return false
            }
            if (!messageObject?.object || messageObject?.object === undefined) {
                channel.ack(payload)
                return false
            }
            const vendorPNMessageModel = await SqlDB.VendorPNMessageModel.findOne({
                where: {
                    id: messageObject.object,
                },
                include: [{
                    model: SqlDB.ContractModel,
                    required: true,
                    include: {
                        model: SqlDB.UserModel,
                        where: {
                            role: {
                                [SqlDB.Op.in]: [Roles.CLIENT]
                            }
                        },
                        include: {
                            model: SqlDB.PNModel,
                            required: true,
                        },
                        required: false,
                    }
                }]
            })
            if (!vendorPNMessageModel) {
                channel.ack(payload)
                this.logger.log(` [Erro]: Não foi localizado um estabelecimento!!`)
                return false
            }
            const notification = new Notification(Notification.VENDOR_MESSAGE)
            notification.title = vendorPNMessageModel?.title
            notification.body = vendorPNMessageModel?.body
            const message = {
                notification,
                data: {
                    method: notification?.method,
                    uri: notification?.uri,
                    logon: notification?.logon,
                    payload: null,
                },
                token: null
            }
            let sends = 0
            let fails = 0
            for (let userModel of (vendorPNMessageModel?.contract?.users ?? [])) {
                const pNModel = userModel?.pN
                const pNMessageModel = await pNModel.createPNMessage({
                    title: message?.notification?.title,
                    body: message?.notification?.body,
                })
                await pNMessageModel.setVendorPNMessage(vendorPNMessageModel)
                await pNMessageModel.setContract(vendorPNMessageModel?.contract)
                await pNMessageModel.setUser(userModel)
                message.data.payload = pNMessageModel?.id
                message.token = pNModel?.token
                message.id = pNMessageModel?.id
                message.priority = 5
                message.ikomidaId = vendorPNMessageModel?.contract?.ikomidaId
                const response = await this.sendPushNotificationByToken(pNMessageModel, message, pNModel?.platform)
                switch (response?.code) {
                    case 0:
                        sends++
                        this.logger.log(` [x] Push notification enviado com sucesso`)
                        break
                    case 1:
                        fails++
                        this.logger.warn(` [x] Push notification não foi enviado, token não foi localizado`)
                        await pNModel?.destroy()
                        break
                }
            }
            vendorPNMessageModel.sends = sends
            vendorPNMessageModel.fails = fails
            await vendorPNMessageModel.save()
            channel.ack(payload)
        } catch (error) {
            console.error(error)
        }
        return false
    }

    async sendPushNotificationByToken(model, payload, platform) {
        let response = { code: -1 }
        try {
            if (platform === 'android') {
                response = await this.googleAdmin.sendPushNotification(payload)
            } else {
                response = await this.appleAPNs.sendPushNotification(payload)
            }
            if (response?.code === 0) {
                model.remoteId = response?.id
                model.send = true
                model.save()
            }
        } catch (error) {
            this.logger.log("payload:", payload)
            this.logger.error(error)
        }
        return response
    }
}

await (new VendorPushNotificationWorker).run()