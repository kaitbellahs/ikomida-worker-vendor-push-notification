import { Types, Utils, Domain, DBModels, BackendTypes } from "@ikomida/shared-backend"
import { Channel, ConsumeMessage } from "amqplib"
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
    .replace(/^\w/, (m: string) => m.toUpperCase())
    .replace(/-\w/g, (m: string[]) => m[1].toUpperCase())

class VendorPushNotificationWorker {

    googleAdmin?: Utils.GoogleAdmin
    appleAPNs?: Utils.AppleAPNs
    amqp?: Domain.RabbitMQ
    logger

    constructor() {
        this.logger = Utils.Logger.getInstance(name)
    }

    async run() {
        try {
            this.googleAdmin = new Utils.GoogleAdmin(this.logger)
            this.appleAPNs = new Utils.AppleAPNs(this.logger)
            this.amqp = new Domain.RabbitMQ(this.logger)
            await this.amqp.listenToMessages(Domain.RabbitMQ.VENDOR_PUSH_NOTIFICATION_QUEUE, this.processMessages.bind(this))
        } catch (error: any) {
            this.logger.error(error)
        }
    }

    async processMessages(message: ConsumeMessage, channel: Channel) {
        try {
            this.logger.log(` [x] ${message.fields.routingKey}: message received: '${message.content.toString('utf8')}'`)
            const payloadObject: Types.Classes.CAMQPPayload<string> = Types.Classes.CAMQPPayload.fromObject(JSON.parse(message.content.toString('utf8')))
            if (payloadObject.method !== 'sendVendorPushNotifications') {
                this.logger.log(` [Error]: O metodo: ${payloadObject?.method} não suportado!`)
                channel.ack(message)
                return false
            }
            if (!payloadObject?.object || payloadObject?.object === undefined) {
                channel.ack(message)
                return false
            }
            const vendorPNMessageModel = await DBModels.VendorPNMessageModel.findOne({
                where: {
                    id: String(payloadObject.object),
                },
                include: [{
                    model: DBModels.ContractModel,
                    required: true,
                    include: [{
                        model: DBModels.UserModel,
                        where: {
                            role: {
                                [Domain.SqlDB.Op.in]: [BackendTypes.Roles.CLIENT.id]
                            }
                        },
                        include: [{
                            model: DBModels.PNModel,
                            required: true,
                        }],
                        required: false,
                    }]
                }]
            })
            if (!vendorPNMessageModel) {
                channel.ack(message)
                this.logger.log(` [Erro]: Não foi localizado um estabelecimento!!`)
                return false
            }
            const notification = new Utils.Notification(Utils.Notification.VENDOR_MESSAGE)
            notification.title = vendorPNMessageModel?.title ?? ''
            notification.body = vendorPNMessageModel?.body ?? ''
            const payload: Types.Classes.CNotificationPayload = Types.Classes.CNotificationPayload.fromObject({
                notification,
                data: Types.Classes.CNotificationData.fromObject({
                    method: notification?.method,
                    uri: notification?.uri,
                    logon: notification?.logon,
                    message: null,
                }),
                token: null
            })
            let sends = 0
            let fails = 0
            for (const userModel of (vendorPNMessageModel?.contract?.users ?? [])) {
                const pNModel = userModel?.pN
                const pNMessageModel: DBModels.PNMessageModel | undefined = await pNModel?.$create('pNMessage', {
                    title: payload?.notification?.title,
                    body: payload?.notification?.body,
                })
                if (pNMessageModel) {
                    await vendorPNMessageModel.$add('pNMessage', pNMessageModel)
                    await vendorPNMessageModel?.contract?.$add('pNMessage', pNMessageModel)
                    await userModel.$add('pNMessage', pNMessageModel)
                }
                if (payload.data) {
                    payload.data.message = pNMessageModel?.id
                }
                payload.token = pNModel?.token ?? ''
                payload.id = pNMessageModel?.id
                payload.priority = 5
                payload.ikomidaId = vendorPNMessageModel?.contract?.ikomidaID
                const response = await this.sendPushNotificationByToken(pNMessageModel, payload, pNModel?.platform)
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
            channel.ack(message)
        } catch (error: any) {
            console.error(error)
        }
        return false
    }

    async sendPushNotificationByToken(model?: DBModels.PNMessageModel, message?: Types.Classes.CNotificationPayload, platform?: string) {
        let response: Types.Types.TSendReturn = { code: -1 }
        try {
            if (platform === 'android') {
                response = await this.googleAdmin?.sendPushNotification(message)
            } else {
                response = await this.appleAPNs?.sendPushNotification(message)
            }
            if (response?.code === 0 && model) {
                model.remoteId = response?.id
                model.send = true
                model.save()
            }
        } catch (error: any) {
            this.logger.log("message:", message)
            this.logger.error(error)
        }
        return response
    }
}

await (new VendorPushNotificationWorker).run()