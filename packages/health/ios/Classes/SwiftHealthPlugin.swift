import Flutter
import UIKit
import HealthKit

public class SwiftHealthPlugin: NSObject, FlutterPlugin {
    static let logStreamHandler = LogStreamHandler()
    
    let healthStore = HKHealthStore()
    var healthDataTypes = [HKSampleType]()
    var characteristicsDataTypes = [HKCharacteristicType]()
    var heartRateEventTypes = Set<HKSampleType>()
    var headacheType = Set<HKSampleType>()
    var allDataTypes = Set<HKSampleType>()
    var dataTypesDict: [String: HKSampleType] = [:]
    var characteristicsTypesDict: [String: HKCharacteristicType] = [:]
    var unitDict: [String: HKUnit] = [:]
    var workoutActivityTypeMap: [String: HKWorkoutActivityType] = [:]
    
    
    // Health Data Type Keys
    let ACTIVE_ENERGY_BURNED = "ACTIVE_ENERGY_BURNED"
    let AUDIOGRAM = "AUDIOGRAM"
    let BASAL_ENERGY_BURNED = "BASAL_ENERGY_BURNED"
    let BLOOD_GLUCOSE = "BLOOD_GLUCOSE"
    let BLOOD_OXYGEN = "BLOOD_OXYGEN"
    let BLOOD_PRESSURE_DIASTOLIC = "BLOOD_PRESSURE_DIASTOLIC"
    let BLOOD_PRESSURE_SYSTOLIC = "BLOOD_PRESSURE_SYSTOLIC"
    let BODY_FAT_PERCENTAGE = "BODY_FAT_PERCENTAGE"
    let BODY_MASS_INDEX = "BODY_MASS_INDEX"
    let BODY_TEMPERATURE = "BODY_TEMPERATURE"
    let DIETARY_CARBS_CONSUMED = "DIETARY_CARBS_CONSUMED"
    let DIETARY_ENERGY_CONSUMED = "DIETARY_ENERGY_CONSUMED"
    let DIETARY_FATS_CONSUMED = "DIETARY_FATS_CONSUMED"
    let DIETARY_PROTEIN_CONSUMED = "DIETARY_PROTEIN_CONSUMED"
    let ELECTRODERMAL_ACTIVITY = "ELECTRODERMAL_ACTIVITY"
    let FORCED_EXPIRATORY_VOLUME = "FORCED_EXPIRATORY_VOLUME"
    let HEART_RATE = "HEART_RATE"
    let HEART_RATE_VARIABILITY_SDNN = "HEART_RATE_VARIABILITY_SDNN"
    let HEIGHT = "HEIGHT"
    let HIGH_HEART_RATE_EVENT = "HIGH_HEART_RATE_EVENT"
    let IRREGULAR_HEART_RATE_EVENT = "IRREGULAR_HEART_RATE_EVENT"
    let LOW_HEART_RATE_EVENT = "LOW_HEART_RATE_EVENT"
    let RESTING_HEART_RATE = "RESTING_HEART_RATE"
    let BIRTH_DATE = "BIRTH_DATE"
    let GENDER = "GENDER"
    let STEPS = "STEPS"
    let WAIST_CIRCUMFERENCE = "WAIST_CIRCUMFERENCE"
    let WALKING_HEART_RATE = "WALKING_HEART_RATE"
    let WEIGHT = "WEIGHT"
    let DISTANCE_WALKING_RUNNING = "DISTANCE_WALKING_RUNNING"
    let FLIGHTS_CLIMBED = "FLIGHTS_CLIMBED"
    let WATER = "WATER"
    let MINDFULNESS = "MINDFULNESS"
    let SLEEP = "SLEEP"
    let EXERCISE_TIME = "EXERCISE_TIME"
    let WORKOUT = "WORKOUT"
    let HEADACHE_UNSPECIFIED = "HEADACHE_UNSPECIFIED"
    let HEADACHE_NOT_PRESENT = "HEADACHE_NOT_PRESENT"
    let HEADACHE_MILD = "HEADACHE_MILD"
    let HEADACHE_MODERATE = "HEADACHE_MODERATE"
    let HEADACHE_SEVERE = "HEADACHE_SEVERE"
    let MENSTRUATION_DATA = "MENSTRUATION_DATA"
    
    // Health Unit types
    // MOLE_UNIT_WITH_MOLAR_MASS, // requires molar mass input - not supported yet
    // MOLE_UNIT_WITH_PREFIX_MOLAR_MASS, // requires molar mass & prefix input - not supported yet
    let GRAM = "GRAM"
    let KILOGRAM = "KILOGRAM"
    let OUNCE = "OUNCE"
    let POUND = "POUND"
    let STONE = "STONE"
    let METER = "METER"
    let INCH = "INCH"
    let FOOT = "FOOT"
    let YARD = "YARD"
    let MILE = "MILE"
    let LITER = "LITER"
    let MILLILITER = "MILLILITER"
    let FLUID_OUNCE_US = "FLUID_OUNCE_US"
    let FLUID_OUNCE_IMPERIAL = "FLUID_OUNCE_IMPERIAL"
    let CUP_US = "CUP_US"
    let CUP_IMPERIAL = "CUP_IMPERIAL"
    let PINT_US = "PINT_US"
    let PINT_IMPERIAL = "PINT_IMPERIAL"
    let PASCAL = "PASCAL"
    let MILLIMETER_OF_MERCURY = "MILLIMETER_OF_MERCURY"
    let INCHES_OF_MERCURY = "INCHES_OF_MERCURY"
    let CENTIMETER_OF_WATER = "CENTIMETER_OF_WATER"
    let ATMOSPHERE = "ATMOSPHERE"
    let DECIBEL_A_WEIGHTED_SOUND_PRESSURE_LEVEL = "DECIBEL_A_WEIGHTED_SOUND_PRESSURE_LEVEL"
    let SECOND = "SECOND"
    let MILLISECOND = "MILLISECOND"
    let MINUTE = "MINUTE"
    let HOUR = "HOUR"
    let DAY = "DAY"
    let JOULE = "JOULE"
    let KILOCALORIE = "KILOCALORIE"
    let LARGE_CALORIE = "LARGE_CALORIE"
    let SMALL_CALORIE = "SMALL_CALORIE"
    let DEGREE_CELSIUS = "DEGREE_CELSIUS"
    let DEGREE_FAHRENHEIT = "DEGREE_FAHRENHEIT"
    let KELVIN = "KELVIN"
    let DECIBEL_HEARING_LEVEL = "DECIBEL_HEARING_LEVEL"
    let HERTZ = "HERTZ"
    let SIEMEN = "SIEMEN"
    let VOLT = "VOLT"
    let INTERNATIONAL_UNIT = "INTERNATIONAL_UNIT"
    let COUNT = "COUNT"
    let PERCENT = "PERCENT"
    let BEATS_PER_MINUTE = "BEATS_PER_MINUTE"
    let MILLIGRAM_PER_DECILITER = "MILLIGRAM_PER_DECILITER"
    let UNKNOWN_UNIT = "UNKNOWN_UNIT"
    let NO_UNIT = "NO_UNIT"
    
    struct PluginError: Error {
        let message: String
    }
    
    public static func register(with registrar: FlutterPluginRegistrar) {
        let channel = FlutterMethodChannel(name: "flutter_health", binaryMessenger: registrar.messenger())
        let instance = SwiftHealthPlugin()
        registrar.addMethodCallDelegate(instance, channel: channel)
        
        let logChannge = FlutterEventChannel(name: "flutter_health_logs_channel",
                                             binaryMessenger: registrar.messenger())
        logChannge.setStreamHandler(logStreamHandler)
    }
    
    public func handle(_ call: FlutterMethodCall, result: @escaping FlutterResult) {
        // Set up all data types
        initializeTypes()
        
        /// Handle checkIfHealthDataAvailable
        if (call.method.elementsEqual("checkIfHealthDataAvailable")){
            checkIfHealthDataAvailable(call: call, result: result)
        }
        /// Handle requestAuthorization
        else if (call.method.elementsEqual("requestAuthorization")){
            try! requestAuthorization(call: call, result: result)
        }
        
        /// Handle getData
        else if (call.method.elementsEqual("getData")){
            getData(call: call, result: result)
        }
        
        /// Handle getTotalStepsInInterval
        else if (call.method.elementsEqual("getTotalStepsInInterval")){
            getTotalStepsInInterval(call: call, result: result)
        }
        
        /// Handle writeData
        else if (call.method.elementsEqual("writeData")){
            try! writeData(call: call, result: result)
        }
        
        /// Handle writeAudiogram
        else if (call.method.elementsEqual("writeAudiogram")){
            try! writeAudiogram(call: call, result: result)
        }
        
        /// Handle writeWorkoutData
        else if (call.method.elementsEqual("writeWorkoutData")){
            try! writeWorkoutData(call: call, result: result)
        }
        
        /// Handle hasPermission
        else if (call.method.elementsEqual("hasPermissions")){
            try! hasPermissions(call: call, result: result)
        }
    }
    
    func checkIfHealthDataAvailable(call: FlutterMethodCall, result: @escaping FlutterResult) {
        result(HKHealthStore.isHealthDataAvailable())
    }
    
    func hasPermissions(call: FlutterMethodCall, result: @escaping FlutterResult) throws {
        let arguments = call.arguments as? NSDictionary
        guard let types = arguments?["types"] as? Array<String>,
              let permissions = arguments?["permissions"] as? Array<Int>,
              types.count == permissions.count
        else {
            throw PluginError(message: "Invalid Arguments!")
        }
        
        for (index, type) in types.enumerated() {
            if let sampleType = dataTypesDict[type] {
                let success = hasPermission(type: sampleType, access: permissions[index])
                if (success == nil || success == false) {
                    result(success)
                    return
                }
            }
            if let characteristicType = characteristicsTypesDict[type]{
                let success = hasPermission(type: characteristicType, access: permissions[index])
                if (success == nil || success == false) {
                    result(success)
                    return
                }
            }
        }
        
        result(false)
    }
    
    
    func hasPermission(type: HKObjectType, access: Int) -> Bool? {
        
        if #available(iOS 13.0, *) {
            let status = healthStore.authorizationStatus(for: type)
            switch access {
            case 0: // READ
                return nil
            case 1: // WRITE
                return  (status == HKAuthorizationStatus.sharingAuthorized)
            default: // READ_WRITE
                return nil
            }
        }
        else {
            return nil
        }
    }
    
    func requestAuthorization(call: FlutterMethodCall, result: @escaping FlutterResult) throws {
        guard let arguments = call.arguments as? NSDictionary,
              let types = arguments["types"] as? Array<String>,
              let permissions = arguments["permissions"] as? Array<Int>,
              permissions.count == types.count
        else {
            throw PluginError(message: "Invalid Arguments!")
        }
        
        
        var typesToRead = Set<HKObjectType>()
        var typesToWrite = Set<HKSampleType>()
        for (index, key) in types.enumerated() {
            if let dataType = dataTypesDict[key]{
                let access = permissions[index]
                switch access {
                case 0:
                    typesToRead.insert(dataType)
                case 1:
                    typesToWrite.insert(dataType)
                default:
                    break
                }
            }
            if let characteristicsType = characteristicsTypesDict[key]{
                let access = permissions[index]
                switch access {
                case 0:
                    typesToRead.insert(characteristicsType)
                case 1:
                    throw PluginError(message: "Can not ask for writing permissions to the type of \(characteristicsType)")
                default:
                    break
                }
            }
        }
        
        if #available(iOS 13.0, *) {
            healthStore.requestAuthorization(toShare: typesToWrite, read: typesToRead) { (success, error) in
                DispatchQueue.main.async {
                    result(success)
                }
            }
        }
        else {
            result(false)// Handle the error here.
        }
    }
    
    func writeData(call: FlutterMethodCall, result: @escaping FlutterResult) throws {
        guard let arguments = call.arguments as? NSDictionary,
              let value = (arguments["value"] as? Double),
              let type = (arguments["dataTypeKey"] as? String),
              let unit = (arguments["dataUnitKey"] as? String),
              let startTime = (arguments["startTimeSec"] as? NSNumber),
              let endTime = (arguments["endTimeSec"] as? NSNumber)
        else {
            throw PluginError(message: "Invalid Arguments")
        }
        
        let dateFrom = Date(timeIntervalSince1970: startTime.doubleValue)
        let dateTo = Date(timeIntervalSince1970: endTime.doubleValue)
        
        let sample: HKObject
        
        if (unitLookUp(key: type) == HKUnit.init(from: "")) {
            sample = HKCategorySample(type: dataTypesDict[type] as! HKCategoryType, value: Int(value), start: dateFrom, end: dateTo)
        } else {
            let quantity = HKQuantity(unit: unitDict[unit]!, doubleValue: value)
            
            sample = HKQuantitySample(type: dataTypesDict[type] as! HKQuantityType, quantity: quantity, start: dateFrom, end: dateTo)
        }
        
        HKHealthStore().save(sample, withCompletion: { (success, error) in
            if let err = error {
                print("Error Saving \(type) Sample: \(err.localizedDescription)")
            }
            DispatchQueue.main.async {
                result(success)
            }
        })
    }
    
    func writeAudiogram(call: FlutterMethodCall, result: @escaping FlutterResult) throws {
        guard let arguments = call.arguments as? NSDictionary,
              let frequencies = (arguments["frequencies"] as? Array<Double>),
              let leftEarSensitivities = (arguments["leftEarSensitivities"] as? Array<Double>),
              let rightEarSensitivities = (arguments["rightEarSensitivities"] as? Array<Double>),
              let startTime = (arguments["startTimeSec"] as? NSNumber),
              let endTime = (arguments["endTimeSec"] as? NSNumber)
        else {
            throw PluginError(message: "Invalid Arguments")
        }
        
        let dateFrom = Date(timeIntervalSince1970: startTime.doubleValue)
        let dateTo = Date(timeIntervalSince1970: endTime.doubleValue)
        
        var sensitivityPoints = [HKAudiogramSensitivityPoint]()
        
        for index in 0...frequencies.count-1 {
            let frequency = HKQuantity(unit: HKUnit.hertz(), doubleValue: frequencies[index])
            let dbUnit = HKUnit.decibelHearingLevel()
            let left = HKQuantity(unit: dbUnit, doubleValue: leftEarSensitivities[index])
            let right = HKQuantity(unit: dbUnit, doubleValue: rightEarSensitivities[index])
            let sensitivityPoint = try HKAudiogramSensitivityPoint(frequency: frequency,  leftEarSensitivity: left, rightEarSensitivity: right)
            sensitivityPoints.append(sensitivityPoint)
        }
        
        let audiogram: HKAudiogramSample;
        let metadataReceived = (arguments["metadata"] as? [String: Any]?)
        
        if((metadataReceived) != nil) {
            guard let deviceName = metadataReceived?!["HKDeviceName"] as? String else { return }
            guard let externalUUID = metadataReceived?!["HKExternalUUID"] as? String else { return }
            
            audiogram = HKAudiogramSample(sensitivityPoints:sensitivityPoints, start: dateFrom, end: dateTo, metadata: [HKMetadataKeyDeviceName: deviceName, HKMetadataKeyExternalUUID: externalUUID])
            
        } else {
            audiogram = HKAudiogramSample(sensitivityPoints:sensitivityPoints, start: dateFrom, end: dateTo, metadata: nil)
        }
        
        HKHealthStore().save(audiogram, withCompletion: { (success, error) in
            if let err = error {
                print("Error Saving Audiogram. Sample: \(err.localizedDescription)")
            }
            DispatchQueue.main.async {
                result(success)
            }
        })
    }
    
    func writeWorkoutData(call: FlutterMethodCall, result: @escaping FlutterResult) throws {
        guard let arguments = call.arguments as? NSDictionary,
              let activityType = (arguments["activityType"] as? String),
              let startTime = (arguments["startTimeSec"] as? NSNumber),
              let endTime = (arguments["endTimeSec"] as? NSNumber),
              let ac = workoutActivityTypeMap[activityType]
        else {
            throw PluginError(message: "Invalid Arguments - ActivityType, startTime or endTime invalid")
        }
        
        var totalEnergyBurned: HKQuantity?
        var totalDistance: HKQuantity? = nil
        
        // Handle optional arguments
        if let teb = (arguments["totalEnergyBurned"] as? Double) {
            totalEnergyBurned = HKQuantity(unit: unitDict[(arguments["totalEnergyBurnedUnit"] as! String)]!, doubleValue: teb)
        }
        if let td = (arguments["totalDistance"] as? Double) {
            totalDistance = HKQuantity(unit: unitDict[(arguments["totalDistanceUnit"] as! String)]!, doubleValue: td)
        }
        
       let dateFrom = Date(timeIntervalSince1970: startTime.doubleValue)
       let dateTo = Date(timeIntervalSince1970: endTime.doubleValue)
        
        var workout: HKWorkout
        
        workout = HKWorkout(activityType: ac, start: dateFrom, end: dateTo, duration: dateTo.timeIntervalSince(dateFrom),
                            totalEnergyBurned: totalEnergyBurned ?? nil,
                            totalDistance: totalDistance ?? nil, metadata: nil)
        
        HKHealthStore().save(workout, withCompletion: { (success, error) in
            if let err = error {
                print("Error Saving Workout. Sample: \(err.localizedDescription)")
            }
            DispatchQueue.main.async {
                result(success)
            }
        })
    }
    
    private func strFrom(date: Date) -> String {
        // see https://www.advancedswift.com/local-utc-date-format-swift/
        let localISOFormatter = ISO8601DateFormatter()
        localISOFormatter.timeZone = TimeZone.current
        return localISOFormatter.string(from: date)
    }
    
    private func jsonValueFrom(key: String, value: Any?) -> [String: Any]? {
        guard let value = value else {return nil}
        
        // we might have a metadata value that is not serializeble into json
        // We cannot check NSInvalidArgumentException in Swift. Might have to
        // write a ObjC code for that. See:
        // https://stackoverflow.com/a/43586083
        // So, for now we just retun the string representation of the value
        
        return [key : "\(value)"]
    }
    
    func getData(call: FlutterMethodCall, result: @escaping FlutterResult) {
        let arguments = call.arguments as? NSDictionary
        let dataUnitKey = (arguments?["dataUnitKey"] as? String)
        let limit = (arguments?["limit"] as? Int) ?? HKObjectQueryNoLimit
        
        guard let dataTypeKey = (arguments?["dataTypeKey"] as? String) else {
            SwiftHealthPlugin.logStreamHandler.sendError("dataTypeKey is missing from getData request")
            result(nil)
            return
        }
        
        switch(dataTypeKey){
        case BIRTH_DATE:
            let dateOfBirth = getDOB()
            result([
                ["value": dateOfBirth?.timeIntervalSince1970]
            ])
            return
        case GENDER:
            let gender = getBioLogicalSex()
            result([
                ["value": gender?.rawValue]
            ])
            return
        default:
            break
        }
        
        guard let startTime = (arguments?["startTimeSec"] as? NSNumber),
                let endTime = (arguments?["endTimeSec"] as? NSNumber) else {
            SwiftHealthPlugin.logStreamHandler.sendError("start time or end time are missing from getData request")
            result(nil)
            return
        }
        
        // Convert dates from milliseconds to Date()
        let dateFrom = Date(timeIntervalSince1970: startTime.doubleValue)
        let dateTo = Date(timeIntervalSince1970: endTime.doubleValue)
        
        guard let dataType = dataTypesDict[dataTypeKey] else {
            SwiftHealthPlugin.logStreamHandler.sendError("Can not find dataType \(dataTypeKey) in the available data types")
            DispatchQueue.main.async {
                result(nil)
            }
            return
        }
        
        let predicate = HKQuery.predicateForSamples(withStart: dateFrom, end: dateTo, options: .strictStartDate)
        let sortDescriptor = NSSortDescriptor(key: HKSampleSortIdentifierEndDate, ascending: false)
        
        let query = HKSampleQuery(sampleType: dataType, predicate: predicate, limit: limit, sortDescriptors: [sortDescriptor]) { [self]
            x, samplesOrNil, error in
            
            switch samplesOrNil {
            case let (samples as [HKQuantitySample]) as Any:
                guard let dataUnitKey = dataUnitKey else {
                    SwiftHealthPlugin.logStreamHandler.sendError("dataUnitKey is missing from getData request")
                    result(nil)
                    return
                }
                let unit = unitDict[dataUnitKey]
                let dictionaries = samples.map { sample -> NSDictionary in
                    return [
                        "uuid": "\(sample.uuid)",
                        "value": sample.quantity.doubleValue(for: unit!),
                        "startTime": strFrom(date: sample.startDate),
                        "endTime": strFrom(date: sample.endDate),
                        "sourceBundleIdentifier": sample.sourceRevision.source.bundleIdentifier,
                        "sourceName": sample.sourceRevision.source.name
                    ]
                }
                DispatchQueue.main.async {
                    result(dictionaries)
                }
                
            case var (samplesCategory as [HKCategorySample]) as Any:
                if (dataTypeKey == self.HEADACHE_UNSPECIFIED) {
                    samplesCategory = samplesCategory.filter { $0.value == 0 }
                }
                if (dataTypeKey == self.HEADACHE_NOT_PRESENT) {
                    samplesCategory = samplesCategory.filter { $0.value == 1 }
                }
                if (dataTypeKey == self.HEADACHE_MILD) {
                    samplesCategory = samplesCategory.filter { $0.value == 2 }
                }
                if (dataTypeKey == self.HEADACHE_MODERATE) {
                    samplesCategory = samplesCategory.filter { $0.value == 3 }
                }
                if (dataTypeKey == self.HEADACHE_SEVERE) {
                    samplesCategory = samplesCategory.filter { $0.value == 4 }
                }
                let categories = samplesCategory.map { sample -> NSDictionary in
                    return [
                        "uuid": "\(sample.uuid)",
                        "value": sample.value,
                        "startTime": strFrom(date: sample.startDate),
                        "endTime": strFrom(date: sample.endDate),
                        "sourceBundleIdentifier": sample.sourceRevision.source.bundleIdentifier,
                        "sourceName": sample.sourceRevision.source.name
                    ]
                }
                DispatchQueue.main.async {
                    result(categories)
                }
                
            case let (samplesWorkout as [HKWorkout]) as Any:
                
                let dictionaries = samplesWorkout.map { sample -> [String: Any] in
                    var workout: [String: Any] = [ "uuid": "\(sample.uuid)",
                                    "workoutActivityType": sample.workoutActivityType.rawValue,
                                    "startTime": strFrom(date: sample.startDate),
                                    "endTime": strFrom(date: sample.endDate),
                                    "sourceBundleIdentifier": sample.sourceRevision.source.bundleIdentifier,
                                    "sourceName": sample.sourceRevision.source.name]
                    if let workoutActivityType = workoutActivityTypeMap.first(where: {$0.value == sample.workoutActivityType})?.key {
                        workout["workoutActivityType"] = workoutActivityType
                    }
                    if let totalEnergyBurned = sample.totalEnergyBurned?.doubleValue(for: HKUnit.kilocalorie()) {
                        workout["totalEnergyBurned"] = totalEnergyBurned
                    }
                    if let totalDistance = sample.totalDistance?.doubleValue(for: HKUnit.meter()) {
                        workout["totalDistance"] = totalDistance / 1000
                    }

                    if let metadata = sample.metadata {
                        var metadataVal: [String: Any] = [:]
                        metadata.keys.forEach { key in
                            if let jsonForKey = jsonValueFrom(key: key, value: metadata[key]), let value = jsonForKey[key] {
                                metadataVal[key] = value
                            }
                        }
                        workout["metadata"] = metadataVal
                    }

                    return workout
                }
                
                DispatchQueue.main.async {
                    result(dictionaries)
                }
                
            case let (samplesAudiogram as [HKAudiogramSample]) as Any:
                let dictionaries = samplesAudiogram.map { sample -> NSDictionary in
                    var frequencies = [Double]()
                    var leftEarSensitivities = [Double]()
                    var rightEarSensitivities = [Double]()
                    for samplePoint in sample.sensitivityPoints {
                        frequencies.append(samplePoint.frequency.doubleValue(for: HKUnit.hertz()))
                        leftEarSensitivities.append(samplePoint.leftEarSensitivity!.doubleValue(for: HKUnit.decibelHearingLevel()))
                        rightEarSensitivities.append(samplePoint.rightEarSensitivity!.doubleValue(for: HKUnit.decibelHearingLevel()))
                    }
                    return [
                        "uuid": "\(sample.uuid)",
                        "frequencies": frequencies,
                        "leftEarSensitivities": leftEarSensitivities,
                        "rightEarSensitivities": rightEarSensitivities,
                        "startTime": strFrom(date: sample.startDate),
                        "endTime": strFrom(date: sample.endDate),
                        "sourceBundleIdentifier": sample.sourceRevision.source.bundleIdentifier,
                        "sourceName": sample.sourceRevision.source.name
                    ]
                }
                DispatchQueue.main.async {
                    result(dictionaries)
                }
                
            default:
                DispatchQueue.main.async {
                    result(nil)
                }
            }
        }
        
        HKHealthStore().execute(query)
    }
    
    func getTotalStepsInInterval(call: FlutterMethodCall, result: @escaping FlutterResult) {
        let arguments = call.arguments as? NSDictionary
        guard let startTime = (arguments?["startTimeSec"] as? NSNumber),
              let endTime = (arguments?["endTimeSec"] as? NSNumber) else {
            SwiftHealthPlugin.logStreamHandler.sendError("start time or end time are missing from getTotalStepsInInterval request")
            result(nil)
            return
        }
        
        // Convert dates from milliseconds to Date()
        let dateFrom = Date(timeIntervalSince1970: startTime.doubleValue)
        let dateTo = Date(timeIntervalSince1970: endTime.doubleValue)
        
        let sampleType = HKQuantityType.quantityType(forIdentifier: .stepCount)!
        let predicate = HKQuery.predicateForSamples(withStart: dateFrom, end: dateTo, options: .strictStartDate)
        
        let query = HKStatisticsQuery(quantityType: sampleType,
                                      quantitySamplePredicate: predicate,
                                      options: .cumulativeSum) { query, queryResult, error in
            
            guard let queryResult = queryResult else {
                let error = error! as NSError
                print("Error getting total steps in interval \(error.localizedDescription)")
                
                DispatchQueue.main.async {
                    result(nil)
                }
                return
            }
            
            var steps = 0.0
            
            if let quantity = queryResult.sumQuantity() {
                let unit = HKUnit.count()
                steps = quantity.doubleValue(for: unit)
            }
            
            let totalSteps = Int(steps)
            DispatchQueue.main.async {
                result(totalSteps)
            }
        }
        
        HKHealthStore().execute(query)
    }
    
    func unitLookUp(key: String) -> HKUnit {
        guard let unit = unitDict[key] else {
            return HKUnit.count()
        }
        return unit
    }
    
    func getBioLogicalSex() -> HKBiologicalSex? {
        var bioSex:HKBiologicalSex?
        do {
            bioSex = try self.healthStore.biologicalSex().biologicalSex
        } catch _{
            bioSex = nil
        }
        return bioSex
    }
    
    func getDOB() -> Date?{
        var dob:Date?
        do {
          dob = try self.healthStore.dateOfBirthComponents().date
        } catch _{
          dob = nil
        }
        return dob
    }
    
    func initializeTypes() {
        // Initialize units
        unitDict[GRAM] = HKUnit.gram()
        unitDict[KILOGRAM] = HKUnit.gramUnit(with: .kilo)
        unitDict[OUNCE] = HKUnit.ounce()
        unitDict[POUND] = HKUnit.pound()
        unitDict[STONE] = HKUnit.stone()
        unitDict[METER] = HKUnit.meter()
        unitDict[INCH] = HKUnit.inch()
        unitDict[FOOT] = HKUnit.foot()
        unitDict[YARD] = HKUnit.yard()
        unitDict[MILE] = HKUnit.mile()
        unitDict[LITER] = HKUnit.liter()
        unitDict[MILLILITER] = HKUnit.literUnit(with: .milli)
        unitDict[FLUID_OUNCE_US] = HKUnit.fluidOunceUS()
        unitDict[FLUID_OUNCE_IMPERIAL] = HKUnit.fluidOunceImperial()
        unitDict[CUP_US] = HKUnit.cupUS()
        unitDict[CUP_IMPERIAL] = HKUnit.cupImperial()
        unitDict[PINT_US] = HKUnit.pintUS()
        unitDict[PINT_IMPERIAL] = HKUnit.pintImperial()
        unitDict[PASCAL] = HKUnit.pascal()
        unitDict[MILLIMETER_OF_MERCURY] = HKUnit.millimeterOfMercury()
        unitDict[CENTIMETER_OF_WATER] = HKUnit.centimeterOfWater()
        unitDict[ATMOSPHERE] = HKUnit.atmosphere()
        unitDict[DECIBEL_A_WEIGHTED_SOUND_PRESSURE_LEVEL] = HKUnit.decibelAWeightedSoundPressureLevel()
        unitDict[SECOND] = HKUnit.second()
        unitDict[MILLISECOND] = HKUnit.secondUnit(with: .milli)
        unitDict[MINUTE] = HKUnit.minute()
        unitDict[HOUR] = HKUnit.hour()
        unitDict[DAY] = HKUnit.day()
        unitDict[JOULE] = HKUnit.joule()
        unitDict[KILOCALORIE] = HKUnit.kilocalorie()
        unitDict[LARGE_CALORIE] = HKUnit.largeCalorie()
        unitDict[SMALL_CALORIE] = HKUnit.smallCalorie()
        unitDict[DEGREE_CELSIUS] = HKUnit.degreeCelsius()
        unitDict[DEGREE_FAHRENHEIT] = HKUnit.degreeFahrenheit()
        unitDict[KELVIN] = HKUnit.kelvin()
        unitDict[DECIBEL_HEARING_LEVEL] = HKUnit.decibelHearingLevel()
        unitDict[HERTZ] = HKUnit.hertz()
        unitDict[SIEMEN] = HKUnit.siemen()
        unitDict[INTERNATIONAL_UNIT] = HKUnit.internationalUnit()
        unitDict[COUNT] = HKUnit.count()
        unitDict[PERCENT] = HKUnit.percent()
        unitDict[BEATS_PER_MINUTE] = HKUnit.init(from: "count/min")
        unitDict[MILLIGRAM_PER_DECILITER] = HKUnit.init(from: "mg/dL")
        unitDict[UNKNOWN_UNIT] = HKUnit.init(from: "")
        unitDict[NO_UNIT] = HKUnit.init(from: "")
        
        // Initialize workout types
        workoutActivityTypeMap["ARCHERY"] = .archery
        workoutActivityTypeMap["BOWLING"] = .bowling
        workoutActivityTypeMap["FENCING"] = .fencing
        workoutActivityTypeMap["GYMNASTICS"] = .gymnastics
        workoutActivityTypeMap["TRACK_AND_FIELD"] = .trackAndField
        workoutActivityTypeMap["AMERICAN_FOOTBALL"] = .americanFootball
        workoutActivityTypeMap["AUSTRALIAN_FOOTBALL"] = .australianFootball
        workoutActivityTypeMap["BASEBALL"] = .baseball
        workoutActivityTypeMap["BASKETBALL"] = .basketball
        workoutActivityTypeMap["CRICKET"] = .cricket
        workoutActivityTypeMap["DISC_SPORTS"] = .discSports
        workoutActivityTypeMap["HANDBALL"] = .handball
        workoutActivityTypeMap["HOCKEY"] = .hockey
        workoutActivityTypeMap["LACROSSE"] = .lacrosse
        workoutActivityTypeMap["RUGBY"] = .rugby
        workoutActivityTypeMap["SOCCER"] = .soccer
        workoutActivityTypeMap["SOFTBALL"] = .softball
        workoutActivityTypeMap["VOLLEYBALL"] = .volleyball
        workoutActivityTypeMap["PREPARATION_AND_RECOVERY"] = .preparationAndRecovery
        workoutActivityTypeMap["FLEXIBILITY"] = .flexibility
        workoutActivityTypeMap["WALKING"] = .walking
        workoutActivityTypeMap["RUNNING"] = .running
        workoutActivityTypeMap["RUNNING_JOGGING"] = .running // Supported due to combining with Android naming
        workoutActivityTypeMap["RUNNING_SAND"] = .running // Supported due to combining with Android naming
        workoutActivityTypeMap["RUNNING_TREADMILL"] = .running // Supported due to combining with Android naming
        workoutActivityTypeMap["WHEELCHAIR_WALK_PACE"] = .wheelchairWalkPace
        workoutActivityTypeMap["WHEELCHAIR_RUN_PACE"] = .wheelchairRunPace
        workoutActivityTypeMap["BIKING"] = .cycling
        workoutActivityTypeMap["HAND_CYCLING"] = .handCycling
        workoutActivityTypeMap["CORE_TRAINING"] = .coreTraining
        workoutActivityTypeMap["ELLIPTICAL"] = .elliptical
        workoutActivityTypeMap["FUNCTIONAL_STRENGTH_TRAINING"] = .functionalStrengthTraining
        workoutActivityTypeMap["TRADITIONAL_STRENGTH_TRAINING"] = .traditionalStrengthTraining
        workoutActivityTypeMap["CROSS_TRAINING"] = .crossTraining
        workoutActivityTypeMap["MIXED_CARDIO"] = .mixedCardio
        workoutActivityTypeMap["HIGH_INTENSITY_INTERVAL_TRAINING"] = .highIntensityIntervalTraining
        workoutActivityTypeMap["JUMP_ROPE"] = .jumpRope
        workoutActivityTypeMap["STAIR_CLIMBING"] = .stairClimbing
        workoutActivityTypeMap["STAIRS"] = .stairs
        workoutActivityTypeMap["STEP_TRAINING"] = .stepTraining
        workoutActivityTypeMap["FITNESS_GAMING"] = .fitnessGaming
        workoutActivityTypeMap["BARRE"] = .barre
        workoutActivityTypeMap["YOGA"] = .yoga
        workoutActivityTypeMap["MIND_AND_BODY"] = .mindAndBody
        workoutActivityTypeMap["PILATES"] = .pilates
        workoutActivityTypeMap["BADMINTON"] = .badminton
        workoutActivityTypeMap["RACQUETBALL"] = .racquetball
        workoutActivityTypeMap["SQUASH"] = .squash
        workoutActivityTypeMap["TABLE_TENNIS"] = .tableTennis
        workoutActivityTypeMap["TENNIS"] = .tennis
        workoutActivityTypeMap["CLIMBING"] = .climbing
        workoutActivityTypeMap["ROCK_CLIMBING"] = .climbing // Supported due to combining with Android naming
        workoutActivityTypeMap["EQUESTRIAN_SPORTS"] = .equestrianSports
        workoutActivityTypeMap["FISHING"] = .fishing
        workoutActivityTypeMap["GOLF"] = .golf
        workoutActivityTypeMap["HIKING"] = .hiking
        workoutActivityTypeMap["HUNTING"] = .hunting
        workoutActivityTypeMap["PLAY"] = .play
        workoutActivityTypeMap["CROSS_COUNTRY_SKIING"] = .crossCountrySkiing
        workoutActivityTypeMap["CURLING"] = .curling
        workoutActivityTypeMap["DOWNHILL_SKIING"] = .downhillSkiing
        workoutActivityTypeMap["SNOW_SPORTS"] = .snowSports
        workoutActivityTypeMap["SNOWBOARDING"] = .snowboarding
        workoutActivityTypeMap["SKATING"] = .skatingSports
        workoutActivityTypeMap["SKATING_CROSS,"] = .skatingSports // Supported due to combining with Android naming
        workoutActivityTypeMap["SKATING_INDOOR,"] = .skatingSports // Supported due to combining with Android naming
        workoutActivityTypeMap["SKATING_INLINE,"] = .skatingSports // Supported due to combining with Android naming
        workoutActivityTypeMap["PADDLE_SPORTS"] = .paddleSports
        workoutActivityTypeMap["ROWING"] = .rowing
        workoutActivityTypeMap["SAILING"] = .sailing
        workoutActivityTypeMap["SURFING_SPORTS"] = .surfingSports
        workoutActivityTypeMap["SWIMMING"] = .swimming
        workoutActivityTypeMap["WATER_FITNESS"] = .waterFitness
        workoutActivityTypeMap["WATER_POLO"] = .waterPolo
        workoutActivityTypeMap["WATER_SPORTS"] = .waterSports
        workoutActivityTypeMap["BOXING"] = .boxing
        workoutActivityTypeMap["KICKBOXING"] = .kickboxing
        workoutActivityTypeMap["MARTIAL_ARTS"] = .martialArts
        workoutActivityTypeMap["TAI_CHI"] = .taiChi
        workoutActivityTypeMap["WRESTLING"] = .wrestling
        workoutActivityTypeMap["OTHER"] = .other
        
        
        
        // Set up iOS 13 specific types (ordinary health data types)
        if #available(iOS 13.0, *) {
            dataTypesDict[ACTIVE_ENERGY_BURNED] = HKSampleType.quantityType(forIdentifier: .activeEnergyBurned)!
            dataTypesDict[AUDIOGRAM] = HKSampleType.audiogramSampleType()
            dataTypesDict[BASAL_ENERGY_BURNED] = HKSampleType.quantityType(forIdentifier: .basalEnergyBurned)!
            dataTypesDict[BLOOD_GLUCOSE] = HKSampleType.quantityType(forIdentifier: .bloodGlucose)!
            dataTypesDict[BLOOD_OXYGEN] = HKSampleType.quantityType(forIdentifier: .oxygenSaturation)!
            dataTypesDict[BLOOD_PRESSURE_DIASTOLIC] = HKSampleType.quantityType(forIdentifier: .bloodPressureDiastolic)!
            dataTypesDict[BLOOD_PRESSURE_SYSTOLIC] = HKSampleType.quantityType(forIdentifier: .bloodPressureSystolic)!
            dataTypesDict[BODY_FAT_PERCENTAGE] = HKSampleType.quantityType(forIdentifier: .bodyFatPercentage)!
            dataTypesDict[BODY_MASS_INDEX] = HKSampleType.quantityType(forIdentifier: .bodyMassIndex)!
            dataTypesDict[BODY_TEMPERATURE] = HKSampleType.quantityType(forIdentifier: .bodyTemperature)!
            dataTypesDict[DIETARY_CARBS_CONSUMED] = HKSampleType.quantityType(forIdentifier: .dietaryCarbohydrates)!
            dataTypesDict[DIETARY_ENERGY_CONSUMED] = HKSampleType.quantityType(forIdentifier: .dietaryEnergyConsumed)!
            dataTypesDict[DIETARY_FATS_CONSUMED] = HKSampleType.quantityType(forIdentifier: .dietaryFatTotal)!
            dataTypesDict[DIETARY_PROTEIN_CONSUMED] = HKSampleType.quantityType(forIdentifier: .dietaryProtein)!
            dataTypesDict[ELECTRODERMAL_ACTIVITY] = HKSampleType.quantityType(forIdentifier: .electrodermalActivity)!
            dataTypesDict[FORCED_EXPIRATORY_VOLUME] = HKSampleType.quantityType(forIdentifier: .forcedExpiratoryVolume1)!
            dataTypesDict[HEART_RATE] = HKSampleType.quantityType(forIdentifier: .heartRate)!
            dataTypesDict[HEART_RATE_VARIABILITY_SDNN] = HKSampleType.quantityType(forIdentifier: .heartRateVariabilitySDNN)!
            dataTypesDict[HEIGHT] = HKSampleType.quantityType(forIdentifier: .height)!
            dataTypesDict[RESTING_HEART_RATE] = HKSampleType.quantityType(forIdentifier: .restingHeartRate)!
            dataTypesDict[STEPS] = HKSampleType.quantityType(forIdentifier: .stepCount)!
            dataTypesDict[WAIST_CIRCUMFERENCE] = HKSampleType.quantityType(forIdentifier: .waistCircumference)!
            dataTypesDict[WALKING_HEART_RATE] = HKSampleType.quantityType(forIdentifier: .walkingHeartRateAverage)!
            dataTypesDict[WEIGHT] = HKSampleType.quantityType(forIdentifier: .bodyMass)!
            dataTypesDict[DISTANCE_WALKING_RUNNING] = HKSampleType.quantityType(forIdentifier: .distanceWalkingRunning)!
            dataTypesDict[FLIGHTS_CLIMBED] = HKSampleType.quantityType(forIdentifier: .flightsClimbed)!
            dataTypesDict[WATER] = HKSampleType.quantityType(forIdentifier: .dietaryWater)!
            dataTypesDict[MINDFULNESS] = HKSampleType.categoryType(forIdentifier: .mindfulSession)!
            dataTypesDict[MENSTRUATION_DATA] = HKSampleType.categoryType(forIdentifier: .menstrualFlow)!
            dataTypesDict[SLEEP] = HKSampleType.categoryType(forIdentifier: .sleepAnalysis)!
            dataTypesDict[EXERCISE_TIME] = HKSampleType.quantityType(forIdentifier: .appleExerciseTime)!
            dataTypesDict[WORKOUT] = HKSampleType.workoutType()
            healthDataTypes = Array(dataTypesDict.values)
            
            characteristicsTypesDict[BIRTH_DATE] = HKObjectType.characteristicType(forIdentifier: .dateOfBirth)!
            characteristicsTypesDict[GENDER] = HKObjectType.characteristicType(forIdentifier: .biologicalSex)!
            characteristicsDataTypes = Array(characteristicsTypesDict.values)
        }
        // Set up heart rate data types specific to the apple watch, requires iOS 12
        if #available(iOS 12.2, *){
            dataTypesDict[HIGH_HEART_RATE_EVENT] = HKSampleType.categoryType(forIdentifier: .highHeartRateEvent)!
            dataTypesDict[LOW_HEART_RATE_EVENT] = HKSampleType.categoryType(forIdentifier: .lowHeartRateEvent)!
            dataTypesDict[IRREGULAR_HEART_RATE_EVENT] = HKSampleType.categoryType(forIdentifier: .irregularHeartRhythmEvent)!
            
            heartRateEventTypes =  Set([
                HKSampleType.categoryType(forIdentifier: .highHeartRateEvent)!,
                HKSampleType.categoryType(forIdentifier: .lowHeartRateEvent)!,
                HKSampleType.categoryType(forIdentifier: .irregularHeartRhythmEvent)!,
            ])
        }
        
        if #available(iOS 13.6, *){
            dataTypesDict[HEADACHE_UNSPECIFIED] = HKSampleType.categoryType(forIdentifier: .headache)!
            dataTypesDict[HEADACHE_NOT_PRESENT] = HKSampleType.categoryType(forIdentifier: .headache)!
            dataTypesDict[HEADACHE_MILD] = HKSampleType.categoryType(forIdentifier: .headache)!
            dataTypesDict[HEADACHE_MODERATE] = HKSampleType.categoryType(forIdentifier: .headache)!
            dataTypesDict[HEADACHE_SEVERE] = HKSampleType.categoryType(forIdentifier: .headache)!
            
            headacheType = Set([
                HKSampleType.categoryType(forIdentifier: .headache)!,
            ])
        }
        
        if #available(iOS 14.0, *) {
            unitDict[VOLT] = HKUnit.volt()
            unitDict[INCHES_OF_MERCURY] = HKUnit.inchesOfMercury()
            
            workoutActivityTypeMap["CARDIO_DANCE"] = HKWorkoutActivityType.cardioDance
            workoutActivityTypeMap["SOCIAL_DANCE"] = HKWorkoutActivityType.socialDance
            workoutActivityTypeMap["PICKLEBALL"] = HKWorkoutActivityType.pickleball
            workoutActivityTypeMap["COOLDOWN"] = HKWorkoutActivityType.cooldown
        }
        
        // Concatenate heart events, headache and health data types (both may be empty)
        allDataTypes = Set(heartRateEventTypes + healthDataTypes)
        allDataTypes = allDataTypes.union(headacheType)
    }
}

class LogStreamHandler: NSObject, FlutterStreamHandler {
    var eventSink: FlutterEventSink?
    
    public func onListen(withArguments arguments: Any?, eventSink events: @escaping FlutterEventSink) -> FlutterError? {
        eventSink = events
        return nil;
    }
    
    public func sendInfo(_ message: String) {
        eventSink?(message)
    }

    public func sendError(_ message: String) {
        eventSink?(FlutterError(code: "Health_Error", message: message, details: nil))
    }

    public func onCancel(withArguments arguments: Any?) -> FlutterError? {
        eventSink  = nil
        return nil

    }
}
