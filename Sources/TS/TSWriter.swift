import AVFoundation
import CoreMedia
import Foundation

#if canImport(SwiftPMSupport)
import SwiftPMSupport
#endif

/// MPEG-2 TS (Transport Stream) Writer delegate
public protocol TSWriterDelegate: AnyObject {
    func didOutput(_ data: Data)
}

/// MPEG-2 TS (Transport Stream) Writer Foundation class
public class TSWriter: Running {
    public static let defaultPATPID: UInt16 = 0
    public static let defaultPMTPID: UInt16 = 4095
    public static let defaultVideoPID: UInt16 = 256
    public static let defaultAudioPID: UInt16 = 257

    public static let defaultSegmentDuration: Double = 2

    /// The delegate instance.
    public weak var delegate: TSWriterDelegate?
    /// This instance is running to process(true) or not(false).
    public internal(set) var isRunning: Atomic<Bool> = .init(false)
    /// The exptected medias = [.video, .audio].
    public var expectedMedias: Set<AVMediaType> = []

    var audioContinuityCounter: UInt8 = 0
    var videoContinuityCounter: UInt8 = 0
    var PCRPID: UInt16 = TSWriter.defaultVideoPID
    var rotatedTimestamp = CMTime.zero
    var segmentDuration: Double = TSWriter.defaultSegmentDuration
    let lockQueue = DispatchQueue(label: "com.haishinkit.HaishinKit.TSWriter.lock")

    var bufferedSamples: [BufferedSampleBuffer] = []

    let writeLock = NSLock()

    private(set) var PAT: ProgramAssociationSpecific = {
        let PAT: ProgramAssociationSpecific = .init()
        PAT.programs = [1: TSWriter.defaultPMTPID]
        return PAT
    }()
    private(set) var PMT: ProgramMapSpecific = .init()
    private var audioConfig: AudioSpecificConfig? {
        didSet {
            writeProgramIfNeeded()
        }
    }
    private var videoConfig: AVCConfigurationRecord? {
        didSet {
            writeProgramIfNeeded()
        }
    }
    private var videoTimestamp: CMTime = .invalid
    private var audioTimestamp: CMTime = .invalid
    private var lastVideoTimestamp: CMTime = .invalid
    private var PCRTimestamp: CMTime = .zero
    private var canWriteFor: Bool {
        guard expectedMedias.isEmpty else {
            return true
        }
        if expectedMedias.contains(.audio) && expectedMedias.contains(.video) {
            return audioConfig != nil && videoConfig != nil
        }
        if expectedMedias.contains(.video) {
            return videoConfig != nil
        }
        if expectedMedias.contains(.audio) {
            return audioConfig != nil
        }
        return false
    }

    public init(segmentDuration: Double = TSWriter.defaultSegmentDuration) {
        self.segmentDuration = segmentDuration
    }

    public func startRunning() {
        guard isRunning.value else {
            return
        }

        isRunning.mutate { $0 = true }
    }

    public func stopRunning() {
        guard !isRunning.value else {
            return
        }
        audioContinuityCounter = 0
        videoContinuityCounter = 0
        PCRPID = TSWriter.defaultVideoPID
        PAT.programs.removeAll()
        PAT.programs = [1: TSWriter.defaultPMTPID]
        PMT = ProgramMapSpecific()
        audioConfig = nil
        videoConfig = nil
        videoTimestamp = .invalid
        audioTimestamp = .invalid
        PCRTimestamp = .invalid

        isRunning.mutate { $0 = false }
    }

    struct BufferedSampleBuffer {
        let pid: UInt16
        let streamID: UInt8
        var bytes: [UInt8]
        let pts: CMTime
        let dts: CMTime
        let randomAccessIndicator: Bool

        init(pid: UInt16, streamID: UInt8, bytes: UnsafePointer<UInt8>?, count: UInt32, pts: CMTime, dts: CMTime, rai: Bool) {
            self.pid = pid
            self.streamID = streamID
            if bytes != nil {
                self.bytes = [UInt8](repeating: 0, count: Int(count))
                memcpy(&self.bytes, bytes, Int(count))
            } else {
                self.bytes = []
            }
            self.pts = pts
            self.dts = dts
            self.randomAccessIndicator = rai
        }
    }

    // swiftlint:disable function_parameter_count
    final func writeSampleBuffer(_ PID: UInt16, streamID: UInt8, bytes: UnsafePointer<UInt8>?, count: UInt32, presentationTimeStamp: CMTime, decodeTimeStamp: CMTime, randomAccessIndicator: Bool) {

        self.writeLock.lock()
        defer {
            writeLock.unlock()
        }
        var didWrite = false
        if PID == TSWriter.defaultAudioPID && self.bufferedSamples.count > 0 {
            self.bufferedSamples.append(BufferedSampleBuffer(pid: PID, streamID: streamID, bytes: bytes, count: count, pts: presentationTimeStamp, dts: decodeTimeStamp, rai: randomAccessIndicator))
        } else {
            didWrite = self.writeSampleBufferImpl(PID, streamID: streamID, bytes: bytes, count: count, presentationTimeStamp: presentationTimeStamp, decodeTimeStamp: decodeTimeStamp, randomAccessIndicator: randomAccessIndicator)
        }
        if didWrite && PID == TSWriter.defaultVideoPID {
            lastVideoTimestamp = presentationTimeStamp
        }

        if (didWrite && self.bufferedSamples.count > 0) || (self.bufferedSamples.count > 0 && !didWrite && PID == TSWriter.defaultAudioPID && lastVideoTimestamp != .invalid) {
            // We need to write all buffered samples with timestamps at or after the written video
            // timestamp
            var newBufferedSamples: [BufferedSampleBuffer] = []
            for bufferedSample in self.bufferedSamples {
                if lastVideoTimestamp != .invalid && bufferedSample.pts <= lastVideoTimestamp {
                    let didWriteBuf = self.writeSampleBufferImpl(bufferedSample.pid, streamID: bufferedSample.streamID, bytes: bufferedSample.bytes, count: UInt32(bufferedSample.bytes.count), presentationTimeStamp: bufferedSample.pts, decodeTimeStamp: bufferedSample.dts, randomAccessIndicator: bufferedSample.randomAccessIndicator)
                    if !didWriteBuf {
                        newBufferedSamples.append(bufferedSample)
                    }
                } else {
                    newBufferedSamples.append(bufferedSample)
                }
            }
            bufferedSamples = newBufferedSamples
        }
    }

    private func writeSampleBufferImpl(_ PID: UInt16, streamID: UInt8, bytes: UnsafePointer<UInt8>?, count: UInt32, presentationTimeStamp: CMTime, decodeTimeStamp: CMTime, randomAccessIndicator: Bool) -> Bool {
        guard canWriteFor else {
            return false
        }

        switch PID {
        case TSWriter.defaultAudioPID:
            guard audioTimestamp == .invalid else { break }
            audioTimestamp = presentationTimeStamp
            if PCRPID == PID {
                PCRTimestamp = presentationTimeStamp
            }
        case TSWriter.defaultVideoPID:
            guard videoTimestamp == .invalid else { break }
            videoTimestamp = presentationTimeStamp
            // NOTE: Set the audio timestamp back to invalid so it will set it properly to now.
            if PCRPID == PID {
                PCRTimestamp = presentationTimeStamp
            }
        default:
            break
        }

        if videoTimestamp == .invalid {
            self.bufferedSamples.append(BufferedSampleBuffer(pid: PID, streamID: streamID, bytes: bytes, count: count, pts: presentationTimeStamp, dts: decodeTimeStamp, rai: randomAccessIndicator))
            return false
        }

        if false && presentationTimeStamp < videoTimestamp {
            audioTimestamp = .invalid
            return false
        }

        guard var PES = PacketizedElementaryStream.create(
            bytes,
            count: count,
            presentationTimeStamp: presentationTimeStamp,
            decodeTimeStamp: decodeTimeStamp,
            timestamp: PID == TSWriter.defaultVideoPID ? videoTimestamp : audioTimestamp,
            config: streamID == 192 ? audioConfig : videoConfig,
            randomAccessIndicator: randomAccessIndicator) else {
            return false
        }

        PES.streamID = streamID

        let timestamp = decodeTimeStamp == .invalid ? presentationTimeStamp : decodeTimeStamp
        let packets: [TSPacket] = split(PID, PES: PES, timestamp: timestamp)


        if PID == TSWriter.defaultVideoPID {
            rotateFileHandle(timestamp)
        }

        packets[0].adaptationField?.randomAccessIndicator = randomAccessIndicator

        var bytes = Data()
        for var packet in packets {
            switch PID {
            case TSWriter.defaultAudioPID:
                packet.continuityCounter = audioContinuityCounter
                audioContinuityCounter = (audioContinuityCounter + 1) & 0x0f
            case TSWriter.defaultVideoPID:
                packet.continuityCounter = videoContinuityCounter
                videoContinuityCounter = (videoContinuityCounter + 1) & 0x0f
            default:
                break
            }
            bytes.append(packet.data)
        }

        write(bytes)

        return true
    }

    func rotateFileHandle(_ timestamp: CMTime) {
        let duration: Double = timestamp.seconds - rotatedTimestamp.seconds
        if duration <= segmentDuration {
            return
        }
        writeProgram()
        rotatedTimestamp = timestamp
    }

    func write(_ data: Data) {
        delegate?.didOutput(data)
    }

    final func writeProgram() {
        PMT.PCRPID = PCRPID
        var bytes = Data()
        var packets: [TSPacket] = []
        packets.append(contentsOf: PAT.arrayOfPackets(TSWriter.defaultPATPID))
        packets.append(contentsOf: PMT.arrayOfPackets(TSWriter.defaultPMTPID))
        for packet in packets {
            bytes.append(packet.data)
        }
        write(bytes)
    }

    final func writeProgramIfNeeded() {
        guard !expectedMedias.isEmpty else {
            return
        }
        guard canWriteFor else {
            return
        }
        writeProgram()
    }

    private func split(_ PID: UInt16, PES: PacketizedElementaryStream, timestamp: CMTime) -> [TSPacket] {
        var PCR: UInt64?
        let duration: Double = timestamp.seconds - PCRTimestamp.seconds
        if PCRPID == PID && 0.02 <= duration {
            PCR = UInt64((timestamp.seconds - (PID == TSWriter.defaultVideoPID ? videoTimestamp : audioTimestamp).seconds) * TSTimestamp.resolution)
            PCRTimestamp = timestamp
        }
        var packets: [TSPacket] = []
        for packet in PES.arrayOfPackets(PID, PCR: PCR) {
            packets.append(packet)
        }
        return packets
    }
}

extension TSWriter: AudioCodecDelegate {
    // MARK: AudioCodecDelegate
    public func didSetFormatDescription(audio formatDescription: CMFormatDescription?) {
        guard let formatDescription: CMAudioFormatDescription = formatDescription else {
            return
        }
        var data = ElementaryStreamSpecificData()
        data.streamType = ElementaryStreamType.adtsaac.rawValue
        data.elementaryPID = TSWriter.defaultAudioPID
        PMT.elementaryStreamSpecificData.append(data)
        audioContinuityCounter = 0
        audioConfig = AudioSpecificConfig(formatDescription: formatDescription)
    }

    public func sampleOutput(audio data: UnsafeMutableAudioBufferListPointer, presentationTimeStamp: CMTime) {
        guard !data.isEmpty && 0 < data[0].mDataByteSize else {
            return
        }
        writeSampleBuffer(
            TSWriter.defaultAudioPID,
            streamID: 192,
            bytes: data[0].mData?.assumingMemoryBound(to: UInt8.self),
            count: data[0].mDataByteSize,
            presentationTimeStamp: presentationTimeStamp,
            decodeTimeStamp: .invalid,
            randomAccessIndicator: true
        )
    }
}

extension TSWriter: VideoEncoderDelegate {
    // MARK: VideoEncoderDelegate
    public func didSetFormatDescription(video formatDescription: CMFormatDescription?) {
        guard
            let formatDescription: CMFormatDescription = formatDescription,
            let avcC: Data = AVCConfigurationRecord.getData(formatDescription) else {
            return
        }
        var data = ElementaryStreamSpecificData()
        data.streamType = ElementaryStreamType.h264.rawValue
        data.elementaryPID = TSWriter.defaultVideoPID
        PMT.elementaryStreamSpecificData.append(data)
        videoContinuityCounter = 0
        videoConfig = AVCConfigurationRecord(data: avcC)
    }

    public func sampleOutput(video sampleBuffer: CMSampleBuffer) {
        guard let dataBuffer = sampleBuffer.dataBuffer else {
            return
        }
        var length: Int = 0
        var buffer: UnsafeMutablePointer<Int8>?
        guard CMBlockBufferGetDataPointer(dataBuffer, atOffset: 0, lengthAtOffsetOut: nil, totalLengthOut: &length, dataPointerOut: &buffer) == noErr else {
            return
        }
        guard let bytes = buffer else {
            return
        }
        writeSampleBuffer(
            TSWriter.defaultVideoPID,
            streamID: 224,
            bytes: UnsafeRawPointer(bytes).bindMemory(to: UInt8.self, capacity: length),
            count: UInt32(length),
            presentationTimeStamp: sampleBuffer.presentationTimeStamp,
            decodeTimeStamp: sampleBuffer.decodeTimeStamp,
            randomAccessIndicator: !sampleBuffer.isNotSync
        )
    }
}

public protocol TSFileWriterDelegate: AnyObject {
    func didStartWritingToFile(at: URL)
    func didFinishWritingToFile(at: URL?, duration: Double, isFinalSegment: Bool)
}

public class TSFileWriter: TSWriter {
    static let defaultSegmentCount: Int = 3
    public static let defaultSegmentMaxCount: Int = 12

    public var segmentMaxCount: Int = TSFileWriter.defaultSegmentMaxCount
    private(set) var files: [M3UMediaInfo] = []
    private var currentFileHandle: FileHandle?
    private var currentFileURL: URL?
    private var sequence: Int = 0

    private var lastTimestamp: CMTime = .invalid

    private var fileDirectory: URL

    public weak var fileDelegate: TSFileWriterDelegate?

    public init(segmentDuration: Double = TSWriter.defaultSegmentDuration, segmentMaxCount: Int = TSFileWriter.defaultSegmentMaxCount) {
        #if os(OSX)
        let bundleIdentifier: String? = Bundle.main.bundleIdentifier
        let temp: String = bundleIdentifier == nil ? NSTemporaryDirectory() : NSTemporaryDirectory() + bundleIdentifier! + "/"
        #else
        let temp: String = NSTemporaryDirectory()
        #endif
        self.fileDirectory = URL(fileURLWithPath: temp, isDirectory: true)

        self.segmentMaxCount = segmentMaxCount

        super.init(segmentDuration: segmentDuration)
    }

    public init(fileDirectory: URL, segmentDuration: Double = TSWriter.defaultSegmentDuration) {
        self.fileDirectory = fileDirectory

        super.init(segmentDuration: segmentDuration)
    }

    public override func startRunning() {
        if isRunning.value {
            return
        }

        lastTimestamp = .invalid
        rotatedTimestamp = .invalid
        files.removeAll()
        currentFileHandle = nil
        currentFileURL = nil
        sequence = 0

        super.startRunning()
    }

    public var playlist: String {
        var m3u8 = M3U()
        m3u8.targetDuration = segmentDuration
        if sequence <= segmentMaxCount {
            m3u8.mediaSequence = 0
            m3u8.mediaList = files
            for mediaItem in m3u8.mediaList where mediaItem.duration > m3u8.targetDuration {
                m3u8.targetDuration = mediaItem.duration + 1
            }
            return m3u8.description
        }
        let startIndex = max(0, files.count - segmentMaxCount)
        m3u8.mediaSequence = max(0, sequence - segmentMaxCount)
        m3u8.mediaList = Array(files[startIndex..<files.count])
        for mediaItem in m3u8.mediaList where mediaItem.duration > m3u8.targetDuration {
            m3u8.targetDuration = mediaItem.duration + 1
        }
        return m3u8.description
    }

    override func rotateFileHandle(_ timestamp: CMTime) {
        let duration: Double = timestamp.seconds - rotatedTimestamp.seconds
        lastTimestamp = timestamp
        if duration <= segmentDuration {
            return
        }
        let fileManager = FileManager.default

        if !fileManager.fileExists(atPath: self.fileDirectory.path) {
            do {
                try fileManager.createDirectory(atPath: self.fileDirectory.path, withIntermediateDirectories: false, attributes: nil)
            } catch let error as NSError {
                logger.warn("\(error)")
            }
        }

        let filename: String = Int(timestamp.seconds).description + ".ts"
        let url = URL(fileURLWithPath: filename, relativeTo: self.fileDirectory)

        if let currentFileURL: URL = currentFileURL {
            files.append(M3UMediaInfo(url: currentFileURL, duration: duration))
            self.fileDelegate?.didFinishWritingToFile(at: currentFileURL, duration: duration, isFinalSegment: false)
            sequence += 1
        }

        fileManager.createFile(atPath: url.path, contents: nil, attributes: nil)
        if segmentMaxCount <= files.count {
            let info: M3UMediaInfo = files.removeFirst()
            do {
                try fileManager.removeItem(at: info.url as URL)
            } catch let e as NSError {
                logger.warn("\(e)")
            }
        }
        currentFileURL = url
        audioContinuityCounter = 0
        videoContinuityCounter = 0

        nstry({
            self.currentFileHandle?.synchronizeFile()
        }, { exeption in
            logger.warn("\(exeption)")
        })

        currentFileHandle?.closeFile()
        currentFileHandle = try? FileHandle(forWritingTo: url)

        if let newFileURL: URL = currentFileURL {
            self.fileDelegate?.didStartWritingToFile(at: newFileURL)
        }

        writeProgram()
        rotatedTimestamp = timestamp
    }

    override func write(_ data: Data) {
        nstry({
            self.currentFileHandle?.write(data)
        }, { exception in
            self.currentFileHandle?.write(data)
            logger.warn("\(exception)")
        })
        super.write(data)
    }

    public func stopRunning(keepFiles: Bool) {
        guard !isRunning.value else {
            return
        }
        var duration = 0.0
        if lastTimestamp != .invalid && rotatedTimestamp != .invalid {
            duration = lastTimestamp.seconds - rotatedTimestamp.seconds
        }
        if currentFileURL != nil {
            files.append(M3UMediaInfo(url: currentFileURL!, duration: duration))
        }
        fileDelegate?.didFinishWritingToFile(at: currentFileURL, duration: duration, isFinalSegment: true)
        currentFileURL = nil
        currentFileHandle = nil
        if !keepFiles {
            removeFiles()
        }
        super.stopRunning()
    }

    func getFilePath(_ fileName: String) -> String? {
        files.first { $0.url.absoluteString.contains(fileName) }?.url.path
    }

    private func removeFiles() {
        let fileManager = FileManager.default
        for info in files {
            do {
                try fileManager.removeItem(at: info.url as URL)
            } catch let e as NSError {
                logger.warn("\(e)")
            }
        }
        files.removeAll()
    }
}
