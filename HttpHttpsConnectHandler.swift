//
//  HTTP&HTTPSConnectHandler.swift
//  cora-tunnel
//
//  Created by YuriyFpc on 01.02.2022.
//

import NIO
import NIOHTTP1
import OSLog
import CoraCore

final class HttpHttpsConnectHandler {
    private var upgradeState: State
    private let networkConnectionType: ConnectionType

    private var domain: DomainInfo?
    private var bufferedBody: ByteBuffer?
    private var bufferedEnd: HTTPHeaders?

    init(_ networkConnectionType: ConnectionType = NetworkMonitorService.shared.currentConnection) {
        self.upgradeState = .idle
        self.networkConnectionType = networkConnectionType
    }
}

fileprivate extension HttpHttpsConnectHandler {
    enum State {
        case idle
        case beganConnecting
        case awaitingEnd(connectResult: Channel)
        case awaitingConnection(pendingBytes: [NIOAny])
        case upgradeComplete(pendingBytes: [NIOAny])
        case upgradeFailed
        case pendingConnection(head: HTTPRequestHead)
        case connected
    }
}

extension HttpHttpsConnectHandler: ChannelDuplexHandler {
    typealias InboundIn = HTTPServerRequestPart
    typealias InboundOut = HTTPClientRequestPart
    typealias OutboundIn = HTTPClientResponsePart
    typealias OutboundOut = HTTPServerResponsePart

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        switch self.upgradeState {
        case .idle, .pendingConnection(head: _), .connected:
            self.handleInitialMessage(context: context, data: self.unwrapInboundIn(data))

        case .beganConnecting:
            // We got .end, we're still waiting on the connection
            if case .end = self.unwrapInboundIn(data) {
                self.upgradeState = .awaitingConnection(pendingBytes: [])
                self.removeDecoder(context: context)
            }

        case .awaitingEnd(let peerChannel):
            if case .end = self.unwrapInboundIn(data) {
                // Upgrade has completed!
                self.upgradeState = .upgradeComplete(pendingBytes: [])
                self.removeDecoder(context: context)
                self.glue(peerChannel, context: context)
            }

        case .awaitingConnection(var pendingBytes):
            // We've seen end, this must not be HTTP anymore. Danger, Will Robinson! Do not unwrap.
            self.upgradeState = .awaitingConnection(pendingBytes: [])
            pendingBytes.append(data)
            self.upgradeState = .awaitingConnection(pendingBytes: pendingBytes)

        case .upgradeComplete(pendingBytes: var pendingBytes):
            // We're currently delivering data, keep doing so.
            self.upgradeState = .upgradeComplete(pendingBytes: [])
            pendingBytes.append(data)
            self.upgradeState = .upgradeComplete(pendingBytes: pendingBytes)

        case .upgradeFailed:
            break
        }
    }
    
    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        switch self.unwrapOutboundIn(data) {
        case .head(let head):
            context.write(self.wrapOutboundOut(.head(head)), promise: nil)
        case .body(let body):
            context.write(self.wrapOutboundOut(.body(.byteBuffer(body))), promise: nil)
        case .end(let trailers):
            context.write(self.wrapOutboundOut(.end(trailers)), promise: nil)
        }
    }

    func handlerAdded(context: ChannelHandlerContext) {
        // Add logger metadata.
        Logger.proxyService.info("\(String(describing: context.channel.localAddress), privacy: .public) \(#function)")
        Logger.proxyService.info("\(String(describing: context.channel.remoteAddress), privacy: .public) \(#function)")
        Logger.proxyService.info("\(String(describing: ObjectIdentifier(context.channel)), privacy: .public) \(#function)")
    }
}


extension HttpHttpsConnectHandler: RemovableChannelHandler {
    func removeHandler(context: ChannelHandlerContext, removalToken: ChannelHandlerContext.RemovalToken) {
        var didRead = false

        // We are being removed, and need to deliver any pending bytes we may have if we're upgrading.
        while case .upgradeComplete(var pendingBytes) = self.upgradeState, !pendingBytes.isEmpty {
            // Avoid a CoW while we pull some data out.
            self.upgradeState = .upgradeComplete(pendingBytes: [])
            let nextRead = pendingBytes.removeFirst()
            self.upgradeState = .upgradeComplete(pendingBytes: pendingBytes)

            context.fireChannelRead(nextRead)
            didRead = true
        }

        if didRead {
            context.fireChannelReadComplete()
        }

        Logger.proxyService.info("Removing \(String(describing: self), privacy: .public) from pipeline \(#function)")
        context.leavePipeline(removalToken: removalToken)
    }
}

extension HttpHttpsConnectHandler {
    private func handleInitialMessage(context: ChannelHandlerContext, data: InboundIn) {
        guard case .head(let head) = data else {
            // Http collect data
            switch data {
            case .body(let buffer):
                switch upgradeState {
                case .connected:
                    context.fireChannelRead(self.wrapInboundOut(.body(.byteBuffer(buffer))))
                case .pendingConnection:
                    self.bufferedBody = buffer
                default:
                    break
                }
            case .end(let headers):
                switch upgradeState {
                case .connected:
                    context.fireChannelRead(self.wrapInboundOut(.end(headers)))
                case .pendingConnection:
                    self.bufferedEnd = headers
                default:
                    break
                }
            case .head:
                assertionFailure("Not possible")
            }
            
            return
        }

        Logger.proxyService.info("\(String(describing: head.method), privacy: .public) \(String(describing: head.uri), privacy: .public) \(String(describing: head.version), privacy: .public) \(#function)")
        
        // Detect http connection
        if let parsedUrl = URL(string: head.uri), parsedUrl.scheme == "http" {
            Logger.proxyService.info("ParsedUrl \(parsedUrl, privacy: .public), parsedUrl.scheme = \(parsedUrl.scheme ?? "no scheme", privacy: .public)")
            channelReadHttp(context: context, data: data)
            return
        }

        guard head.method == .CONNECT else {
            Logger.proxyService.error("Invalid HTTP method: \(String(describing: head.method), privacy: .public), uri = \(String(describing: head.uri), privacy: .public) \(#function)")
            self.httpErrorAndClose(context: context)
            return
        }

        let components = head.uri.split(separator: ":", maxSplits: 1, omittingEmptySubsequences: false)
        let host = components.first ?? "" // There will always be a first.
        let port = components.last.flatMap { Int($0, radix: 10) } ?? 80  // Port 80 if not specified

        Logger.proxyService.info("Connection current network type: \(String(describing: self.networkConnectionType), privacy: .public) \(#function)")

        self.upgradeState = .beganConnecting
        self.connectTo(host: String(host), port: port, context: context)
    }

    private func connectTo(host: String, port: Int, context: ChannelHandlerContext) {
        Logger.proxyService.info("Connecting to \(String(describing: host), privacy: .public):\(String(describing: port), privacy: .public) \(#function)")

        let channelFuture = ClientBootstrap(group: context.eventLoop)
            .connect(host: String(host), port: port)

        channelFuture.whenSuccess { [weak self] channel in
            self?.connectSucceeded(channel: channel, context: context)
        }
        channelFuture.whenFailure { [weak self] error in
            self?.connectFailed(error: error, context: context)
        }
    }

    private func connectSucceeded(channel: Channel, context: ChannelHandlerContext) {
        Logger.proxyService.info("Connected to \(String(describing: channel.remoteAddress), privacy: .public) \(#function)")

        switch self.upgradeState {
        case .beganConnecting:
            // Ok, we have a channel, let's wait for end.
            self.upgradeState = .awaitingEnd(connectResult: channel)

        case .awaitingConnection(pendingBytes: let pendingBytes):
            // Upgrade complete! Begin gluing the connection together.
            self.upgradeState = .upgradeComplete(pendingBytes: pendingBytes)
            self.glue(channel, context: context)

        case .awaitingEnd(let peerChannel):
            // This case is a logic error, close already connected peer channel.
            peerChannel.close(mode: .all, promise: nil)
            context.close(promise: nil)

        case .idle, .upgradeFailed, .upgradeComplete:
            // These cases are logic errors, but let's be careful and just shut the connection.
            context.close(promise: nil)
        default:
            break
        }
    }

    private func connectFailed(error: Error, context: ChannelHandlerContext) {
        Logger.proxyService.error("Connect failed: \(error.localizedDescription, privacy: .public) \(#function)")

        switch self.upgradeState {
        case .beganConnecting, .awaitingConnection:
            // We still have a somewhat active connection here in HTTP mode, and can report failure.
            self.httpErrorAndClose(context: context)

        case .awaitingEnd(let peerChannel):
            // This case is a logic error, close already connected peer channel.
            peerChannel.close(mode: .all, promise: nil)
            context.close(promise: nil)

        case .idle, .upgradeFailed, .upgradeComplete:
            // Most of these cases are logic errors, but let's be careful and just shut the connection.
            context.close(promise: nil)
        default:
            break
        }

        context.fireErrorCaught(error)
    }

    private func glue(_ peerChannel: Channel, context: ChannelHandlerContext, throttle: Bool = false) {
        Logger.proxyService.debug("Gluing together \(String(describing: ObjectIdentifier(context.channel)), privacy: .public) and \(String(describing: ObjectIdentifier(peerChannel)), privacy: .public) \(#function)")

        // Ok, upgrade has completed! We now need to begin the upgrade process.
        // First, send the 200 message.
        // This content-length header is MUST NOT, but we need to workaround NIO's insistence that we set one.
        let headers = HTTPHeaders([("Content-Length", "0")])
        let head = HTTPResponseHead(version: .init(major: 1, minor: 1), status: .ok, headers: headers)
        context.write(self.wrapOutboundOut(.head(head)), promise: nil)
        context.writeAndFlush(self.wrapOutboundOut(.end(nil)), promise: nil)

        // Now remove the HTTP encoder.
        self.removeEncoder(context: context)

        // Now we need to glue our channel and the peer channel together.
        let (localGlue, peerGlue) = GlueHandler.matchedPair(throttle, domain: domain)
        context.channel.pipeline.addHandler(localGlue).and(peerChannel.pipeline.addHandler(peerGlue)).whenComplete { [weak self] result in
            guard let self = self else { return }
            switch result {
            case .success:
                context.pipeline.removeHandler(self, promise: nil)
            case .failure:
                // Close connected peer channel before closing our channel.
                peerChannel.close(mode: .all, promise: nil)
                context.close(promise: nil)
            }
        }
    }

    private func httpErrorAndClose(context: ChannelHandlerContext) {
        self.upgradeState = .upgradeFailed

        let headers = HTTPHeaders([("Content-Length", "0"), ("Connection", "close")])
        let head = HTTPResponseHead(version: .init(major: 1, minor: 1), status: .badRequest, headers: headers)
        context.write(self.wrapOutboundOut(.head(head)), promise: nil)
        context.writeAndFlush(self.wrapOutboundOut(.end(nil))).whenComplete { (_: Result<Void, Error>) in
            context.close(mode: .output, promise: nil)
        }
    }

    private func removeDecoder(context: ChannelHandlerContext) {
        // We drop the future on the floor here as these handlers must all be in our own pipeline, and this should
        // therefore succeed fast.
        context.pipeline.context(handlerType: ByteToMessageHandler<HTTPRequestDecoder>.self).whenSuccess {
            context.pipeline.removeHandler(context: $0, promise: nil)
        }
    }

    private func removeEncoder(context: ChannelHandlerContext) {
        context.pipeline.context(handlerType: HTTPResponseEncoder.self).whenSuccess {
            context.pipeline.removeHandler(context: $0, promise: nil)
        }
    }
}

// MARK: - HTTPConnectHandler
extension HttpHttpsConnectHandler {
    func sendDataTo(context: ChannelHandlerContext) {
        if case let .pendingConnection(head) = self.upgradeState {
            self.upgradeState = .connected

            context.fireChannelRead(self.wrapInboundOut(.head(head)))
            
            if let bufferedBody = self.bufferedBody {
                context.fireChannelRead(self.wrapInboundOut(.body(.byteBuffer(bufferedBody))))
                self.bufferedBody = nil
            }
            
            if let bufferedEnd = self.bufferedEnd {
                context.fireChannelRead(self.wrapInboundOut(.end(bufferedEnd)))
                self.bufferedEnd = nil
            }
            
            context.fireChannelReadComplete()
        }
    }
    
    enum ConnectError: Error {
        case invalidURL
        case wrongScheme
        case wrongHost
    }
    
    func channelReadHttp(context: ChannelHandlerContext, data: InboundIn) {
        guard case .head(var head) = data else {
            Logger.proxyService.info("Not possible")
            return
        }
        
        Logger.proxyService.info("Connecting to URI: \(head.uri, privacy: .public)")
        
        guard let parsedUrl = URL(string: head.uri) else {
            context.fireErrorCaught(ConnectError.invalidURL)
            return
        }
        
        Logger.proxyService.info("Parsed url: \(parsedUrl.absoluteString, privacy: .public)")

        Logger.proxyService.info("Parsed scheme: \(parsedUrl.scheme ?? "no scheme", privacy: .public)")

        guard let host = head.headers.first(where: { $0.name == "Host" })?.value, let parsedHost = parsedUrl.host, host == parsedHost else {
            Logger.proxyService.info("Wrong host")
            context.fireErrorCaught(ConnectError.wrongHost)
            return
        }
        
        var targetUrl = parsedUrl.path
        
        if let query = parsedUrl.query {
            targetUrl += "?\(query)"
        }
        
        head.uri = targetUrl
        
        switch upgradeState {
        case .idle:
            upgradeState = .pendingConnection(head: head)
            connectToHTTP(host: host, port: 80, context: context)
        case .pendingConnection:
            Logger.proxyService.error("Logic error fireChannelRead with incorrect state")
        case .connected:
            context.fireChannelRead(self.wrapInboundOut(.head(head)))
        default:
            break
        }
    }
    
    private func connectToHTTP(host: String, port: Int, context: ChannelHandlerContext) {
        Logger.proxyService.info("Connecting to \(host, privacy: .public):\(port, privacy: .public)")

        let channelFuture = ClientBootstrap(group: context.eventLoop)
            .channelInitializer { channel in
                channel.pipeline.addHandler(HTTPRequestEncoder()).flatMap {
                    channel.pipeline.addHandler(ByteToMessageHandler(HTTPResponseDecoder(leftOverBytesStrategy: .forwardBytes)))
                }
            }
            .connect(host: host, port: port)
        

        channelFuture.whenSuccess { channel in
            self.connectSucceededHTTP(channel: channel, context: context)
        }
        channelFuture.whenFailure { error in
            self.connectFailedHttp(error: error, context: context)
        }
    }
    
    private func connectSucceededHTTP(channel: Channel, context: ChannelHandlerContext) {
        self.glueHTTP(channel, context: context)
    }

    private func connectFailedHttp(error: Error, context: ChannelHandlerContext) {
        os_log(.error, log: .default, "Connect failed: %@", error as NSError)
        context.fireErrorCaught(error)
    }
    
    private func glueHTTP(_ peerChannel: Channel, context: ChannelHandlerContext) {
        // Now we need to glue our channel and the peer channel together.
        let (localGlue, peerGlue) = GlueHandler.matchedPair(false, domain: nil)
        context.channel.pipeline.addHandler(localGlue).and(peerChannel.pipeline.addHandler(peerGlue)).whenComplete { result in
            switch result {
            case .success:
                self.sendDataTo(context: context)
            case .failure:
                // Close connected peer channel before closing our channel.
                peerChannel.close(mode: .all, promise: nil)
                context.close(promise: nil)
            }
        }
    }
}
