package cachet.plugins.health

import android.app.Activity
import android.content.Context
import android.util.Log
import androidx.activity.ComponentActivity
import androidx.activity.result.ActivityResultLauncher
import androidx.health.connect.client.HealthConnectClient
import androidx.health.connect.client.PermissionController
import androidx.health.connect.client.permission.HealthPermission
import androidx.health.connect.client.records.ActiveCaloriesBurnedRecord
import androidx.health.connect.client.records.BasalMetabolicRateRecord
import androidx.health.connect.client.records.BloodGlucoseRecord
import androidx.health.connect.client.records.BloodPressureRecord
import androidx.health.connect.client.records.BodyFatRecord
import androidx.health.connect.client.records.BodyTemperatureRecord
import androidx.health.connect.client.records.DistanceRecord
import androidx.health.connect.client.records.ExerciseSessionRecord
import androidx.health.connect.client.records.FloorsClimbedRecord
import androidx.health.connect.client.records.HeartRateRecord
import androidx.health.connect.client.records.HeightRecord
import androidx.health.connect.client.records.HydrationRecord
import androidx.health.connect.client.records.OxygenSaturationRecord
import androidx.health.connect.client.records.Record
import androidx.health.connect.client.records.RespiratoryRateRecord
import androidx.health.connect.client.records.RestingHeartRateRecord
import androidx.health.connect.client.records.SleepSessionRecord
import androidx.health.connect.client.records.SleepStageRecord
import androidx.health.connect.client.records.StepsRecord
import androidx.health.connect.client.records.TotalCaloriesBurnedRecord
import androidx.health.connect.client.records.WeightRecord
import androidx.health.connect.client.request.AggregateRequest
import androidx.health.connect.client.request.ReadRecordsRequest
import androidx.health.connect.client.time.TimeRangeFilter
import androidx.health.connect.client.units.BloodGlucose
import androidx.health.connect.client.units.Energy
import androidx.health.connect.client.units.Length
import androidx.health.connect.client.units.Mass
import androidx.health.connect.client.units.Percentage
import androidx.health.connect.client.units.Power
import androidx.health.connect.client.units.Pressure
import androidx.health.connect.client.units.Temperature
import androidx.health.connect.client.units.Volume
import io.flutter.plugin.common.MethodCall
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.time.Instant
import java.time.temporal.ChronoUnit

class HealthConnectService {

    companion object {
        private val mapSleepStageToType = hashMapOf(
            1 to SLEEP_AWAKE,
            2 to SLEEP_ASLEEP,
            3 to SLEEP_OUT_OF_BED,
            4 to SLEEP_LIGHT,
            5 to SLEEP_DEEP,
            6 to SLEEP_REM,
        )

        private val mapToHCType = hashMapOf(
            BODY_FAT_PERCENTAGE to BodyFatRecord::class,
            HEIGHT to HeightRecord::class,
            WEIGHT to WeightRecord::class,
            STEPS to StepsRecord::class,
            AGGREGATE_STEP_COUNT to StepsRecord::class,
            ACTIVE_ENERGY_BURNED to ActiveCaloriesBurnedRecord::class,
            HEART_RATE to HeartRateRecord::class,
            BODY_TEMPERATURE to BodyTemperatureRecord::class,
            BLOOD_PRESSURE_SYSTOLIC to BloodPressureRecord::class,
            BLOOD_PRESSURE_DIASTOLIC to BloodPressureRecord::class,
            BLOOD_OXYGEN to OxygenSaturationRecord::class,
            BLOOD_GLUCOSE to BloodGlucoseRecord::class,
            DISTANCE_DELTA to DistanceRecord::class,
            WATER to HydrationRecord::class,
            SLEEP_ASLEEP to SleepStageRecord::class,
            SLEEP_AWAKE to SleepStageRecord::class,
            SLEEP_LIGHT to SleepStageRecord::class,
            SLEEP_DEEP to SleepStageRecord::class,
            SLEEP_REM to SleepStageRecord::class,
            SLEEP_OUT_OF_BED to SleepStageRecord::class,
            SLEEP_SESSION to SleepSessionRecord::class,
            WORKOUT to ExerciseSessionRecord::class,
            RESTING_HEART_RATE to RestingHeartRateRecord::class,
            BASAL_ENERGY_BURNED to BasalMetabolicRateRecord::class,
            FLIGHTS_CLIMBED to FloorsClimbedRecord::class,
            RESPIRATORY_RATE to RespiratoryRateRecord::class,
            // MOVE_MINUTES to TODO: Find alternative?
            // TODO: Implement remaining types
            // "ActiveCaloriesBurned" to ActiveCaloriesBurnedRecord::class,
            // "BasalBodyTemperature" to BasalBodyTemperatureRecord::class,
            // "BasalMetabolicRate" to BasalMetabolicRateRecord::class,
            // "BloodGlucose" to BloodGlucoseRecord::class,
            // "BloodPressure" to BloodPressureRecord::class,
            // "BodyFat" to BodyFatRecord::class,
            // "BodyTemperature" to BodyTemperatureRecord::class,
            // "BoneMass" to BoneMassRecord::class,
            // "CervicalMucus" to CervicalMucusRecord::class,
            // "CyclingPedalingCadence" to CyclingPedalingCadenceRecord::class,
            // "Distance" to DistanceRecord::class,
            // "ElevationGained" to ElevationGainedRecord::class,
            // "ExerciseSession" to ExerciseSessionRecord::class,
            // "FloorsClimbed" to FloorsClimbedRecord::class,
            // "HeartRate" to HeartRateRecord::class,
            // "Height" to HeightRecord::class,
            // "Hydration" to HydrationRecord::class,
            // "LeanBodyMass" to LeanBodyMassRecord::class,
            // "MenstruationFlow" to MenstruationFlowRecord::class,
            // "MenstruationPeriod" to MenstruationPeriodRecord::class,
            // "Nutrition" to NutritionRecord::class,
            // "OvulationTest" to OvulationTestRecord::class,
            // "OxygenSaturation" to OxygenSaturationRecord::class,
            // "Power" to PowerRecord::class,
            // "RespiratoryRate" to RespiratoryRateRecord::class,
            // "RestingHeartRate" to RestingHeartRateRecord::class,
            // "SexualActivity" to SexualActivityRecord::class,
            // "SleepSession" to SleepSessionRecord::class,
            // "SleepStage" to SleepStageRecord::class,
            // "Speed" to SpeedRecord::class,
            // "StepsCadence" to StepsCadenceRecord::class,
            // "Steps" to StepsRecord::class,
            // "TotalCaloriesBurned" to TotalCaloriesBurnedRecord::class,
            // "Vo2Max" to Vo2MaxRecord::class,
            // "Weight" to WeightRecord::class,
            // "WheelchairPushes" to WheelchairPushesRecord::class,
        )

        // TODO: Update with new workout types when Health Connect becomes the standard.
        val workoutTypeMapHealthConnect = mapOf(
            // "AEROBICS" to ExerciseSessionRecord.EXERCISE_TYPE_AEROBICS,
            "AMERICAN_FOOTBALL" to ExerciseSessionRecord.EXERCISE_TYPE_FOOTBALL_AMERICAN,
            // "ARCHERY" to ExerciseSessionRecord.EXERCISE_TYPE_ARCHERY,
            "AUSTRALIAN_FOOTBALL" to ExerciseSessionRecord.EXERCISE_TYPE_FOOTBALL_AUSTRALIAN,
            "BADMINTON" to ExerciseSessionRecord.EXERCISE_TYPE_BADMINTON,
            "BASEBALL" to ExerciseSessionRecord.EXERCISE_TYPE_BASEBALL,
            "BASKETBALL" to ExerciseSessionRecord.EXERCISE_TYPE_BASKETBALL,
            // "BIATHLON" to ExerciseSessionRecord.EXERCISE_TYPE_BIATHLON,
            "BIKING" to ExerciseSessionRecord.EXERCISE_TYPE_BIKING,
            // "BIKING_HAND" to ExerciseSessionRecord.EXERCISE_TYPE_BIKING_HAND,
            // "BIKING_MOUNTAIN" to ExerciseSessionRecord.EXERCISE_TYPE_BIKING_MOUNTAIN,
            // "BIKING_ROAD" to ExerciseSessionRecord.EXERCISE_TYPE_BIKING_ROAD,
            // "BIKING_SPINNING" to ExerciseSessionRecord.EXERCISE_TYPE_BIKING_SPINNING,
            // "BIKING_STATIONARY" to ExerciseSessionRecord.EXERCISE_TYPE_BIKING_STATIONARY,
            // "BIKING_UTILITY" to ExerciseSessionRecord.EXERCISE_TYPE_BIKING_UTILITY,
            "BOXING" to ExerciseSessionRecord.EXERCISE_TYPE_BOXING,
            "CALISTHENICS" to ExerciseSessionRecord.EXERCISE_TYPE_CALISTHENICS,
            // "CIRCUIT_TRAINING" to ExerciseSessionRecord.EXERCISE_TYPE_CIRCUIT_TRAINING,
            "CRICKET" to ExerciseSessionRecord.EXERCISE_TYPE_CRICKET,
            // "CROSS_COUNTRY_SKIING" to ExerciseSessionRecord.EXERCISE_TYPE_SKIING_CROSS_COUNTRY,
            // "CROSS_FIT" to ExerciseSessionRecord.EXERCISE_TYPE_CROSSFIT,
            // "CURLING" to ExerciseSessionRecord.EXERCISE_TYPE_CURLING,
            "DANCING" to ExerciseSessionRecord.EXERCISE_TYPE_DANCING,
            // "DIVING" to ExerciseSessionRecord.EXERCISE_TYPE_DIVING,
            // "DOWNHILL_SKIING" to ExerciseSessionRecord.EXERCISE_TYPE_SKIING_DOWNHILL,
            // "ELEVATOR" to ExerciseSessionRecord.EXERCISE_TYPE_ELEVATOR,
            "ELLIPTICAL" to ExerciseSessionRecord.EXERCISE_TYPE_ELLIPTICAL,
            // "ERGOMETER" to ExerciseSessionRecord.EXERCISE_TYPE_ERGOMETER,
            // "ESCALATOR" to ExerciseSessionRecord.EXERCISE_TYPE_ESCALATOR,
            "FENCING" to ExerciseSessionRecord.EXERCISE_TYPE_FENCING,
            "FRISBEE_DISC" to ExerciseSessionRecord.EXERCISE_TYPE_FRISBEE_DISC,
            // "GARDENING" to ExerciseSessionRecord.EXERCISE_TYPE_GARDENING,
            "GOLF" to ExerciseSessionRecord.EXERCISE_TYPE_GOLF,
            "GUIDED_BREATHING" to ExerciseSessionRecord.EXERCISE_TYPE_GUIDED_BREATHING,
            "GYMNASTICS" to ExerciseSessionRecord.EXERCISE_TYPE_GYMNASTICS,
            "HANDBALL" to ExerciseSessionRecord.EXERCISE_TYPE_HANDBALL,
            "HIGH_INTENSITY_INTERVAL_TRAINING" to ExerciseSessionRecord.EXERCISE_TYPE_HIGH_INTENSITY_INTERVAL_TRAINING,
            "HIKING" to ExerciseSessionRecord.EXERCISE_TYPE_HIKING,
            // "HOCKEY" to ExerciseSessionRecord.EXERCISE_TYPE_HOCKEY,
            // "HORSEBACK_RIDING" to ExerciseSessionRecord.EXERCISE_TYPE_HORSEBACK_RIDING,
            // "HOUSEWORK" to ExerciseSessionRecord.EXERCISE_TYPE_HOUSEWORK,
            // "IN_VEHICLE" to ExerciseSessionRecord.EXERCISE_TYPE_IN_VEHICLE,
            "ICE_SKATING" to ExerciseSessionRecord.EXERCISE_TYPE_ICE_SKATING,
            // "INTERVAL_TRAINING" to ExerciseSessionRecord.EXERCISE_TYPE_INTERVAL_TRAINING,
            // "JUMP_ROPE" to ExerciseSessionRecord.EXERCISE_TYPE_JUMP_ROPE,
            // "KAYAKING" to ExerciseSessionRecord.EXERCISE_TYPE_KAYAKING,
            // "KETTLEBELL_TRAINING" to ExerciseSessionRecord.EXERCISE_TYPE_KETTLEBELL_TRAINING,
            // "KICK_SCOOTER" to ExerciseSessionRecord.EXERCISE_TYPE_KICK_SCOOTER,
            // "KICKBOXING" to ExerciseSessionRecord.EXERCISE_TYPE_KICKBOXING,
            // "KITE_SURFING" to ExerciseSessionRecord.EXERCISE_TYPE_KITESURFING,
            "MARTIAL_ARTS" to ExerciseSessionRecord.EXERCISE_TYPE_MARTIAL_ARTS,
            // "MEDITATION" to ExerciseSessionRecord.EXERCISE_TYPE_MEDITATION,
            // "MIXED_MARTIAL_ARTS" to ExerciseSessionRecord.EXERCISE_TYPE_MIXED_MARTIAL_ARTS,
            // "P90X" to ExerciseSessionRecord.EXERCISE_TYPE_P90X,
            "PARAGLIDING" to ExerciseSessionRecord.EXERCISE_TYPE_PARAGLIDING,
            "PILATES" to ExerciseSessionRecord.EXERCISE_TYPE_PILATES,
            // "POLO" to ExerciseSessionRecord.EXERCISE_TYPE_POLO,
            "RACQUETBALL" to ExerciseSessionRecord.EXERCISE_TYPE_RACQUETBALL,
            "ROCK_CLIMBING" to ExerciseSessionRecord.EXERCISE_TYPE_ROCK_CLIMBING,
            "ROWING" to ExerciseSessionRecord.EXERCISE_TYPE_ROWING,
            "ROWING_MACHINE" to ExerciseSessionRecord.EXERCISE_TYPE_ROWING_MACHINE,
            "RUGBY" to ExerciseSessionRecord.EXERCISE_TYPE_RUGBY,
            // "RUNNING_JOGGING" to ExerciseSessionRecord.EXERCISE_TYPE_RUNNING_JOGGING,
            // "RUNNING_SAND" to ExerciseSessionRecord.EXERCISE_TYPE_RUNNING_SAND,
            "RUNNING_TREADMILL" to ExerciseSessionRecord.EXERCISE_TYPE_RUNNING_TREADMILL,
            "RUNNING" to ExerciseSessionRecord.EXERCISE_TYPE_RUNNING,
            "SAILING" to ExerciseSessionRecord.EXERCISE_TYPE_SAILING,
            "SCUBA_DIVING" to ExerciseSessionRecord.EXERCISE_TYPE_SCUBA_DIVING,
            // "SKATING_CROSS" to ExerciseSessionRecord.EXERCISE_TYPE_SKATING_CROSS,
            // "SKATING_INDOOR" to ExerciseSessionRecord.EXERCISE_TYPE_SKATING_INDOOR,
            // "SKATING_INLINE" to ExerciseSessionRecord.EXERCISE_TYPE_SKATING_INLINE,
            "SKATING" to ExerciseSessionRecord.EXERCISE_TYPE_SKATING,
            "SKIING" to ExerciseSessionRecord.EXERCISE_TYPE_SKIING,
            // "SKIING_BACK_COUNTRY" to ExerciseSessionRecord.EXERCISE_TYPE_SKIING_BACK_COUNTRY,
            // "SKIING_KITE" to ExerciseSessionRecord.EXERCISE_TYPE_SKIING_KITE,
            // "SKIING_ROLLER" to ExerciseSessionRecord.EXERCISE_TYPE_SKIING_ROLLER,
            // "SLEDDING" to ExerciseSessionRecord.EXERCISE_TYPE_SLEDDING,
            "SNOWBOARDING" to ExerciseSessionRecord.EXERCISE_TYPE_SNOWBOARDING,
            // "SNOWMOBILE" to ExerciseSessionRecord.EXERCISE_TYPE_SNOWMOBILE,
            "SNOWSHOEING" to ExerciseSessionRecord.EXERCISE_TYPE_SNOWSHOEING,
            // "SOCCER" to ExerciseSessionRecord.EXERCISE_TYPE_FOOTBALL_SOCCER,
            "SOFTBALL" to ExerciseSessionRecord.EXERCISE_TYPE_SOFTBALL,
            "SQUASH" to ExerciseSessionRecord.EXERCISE_TYPE_SQUASH,
            "STAIR_CLIMBING_MACHINE" to ExerciseSessionRecord.EXERCISE_TYPE_STAIR_CLIMBING_MACHINE,
            "STAIR_CLIMBING" to ExerciseSessionRecord.EXERCISE_TYPE_STAIR_CLIMBING,
            // "STANDUP_PADDLEBOARDING" to ExerciseSessionRecord.EXERCISE_TYPE_STANDUP_PADDLEBOARDING,
            // "STILL" to ExerciseSessionRecord.EXERCISE_TYPE_STILL,
            "STRENGTH_TRAINING" to ExerciseSessionRecord.EXERCISE_TYPE_STRENGTH_TRAINING,
            "SURFING" to ExerciseSessionRecord.EXERCISE_TYPE_SURFING,
            "SWIMMING_OPEN_WATER" to ExerciseSessionRecord.EXERCISE_TYPE_SWIMMING_OPEN_WATER,
            "SWIMMING_POOL" to ExerciseSessionRecord.EXERCISE_TYPE_SWIMMING_POOL,
            // "SWIMMING" to ExerciseSessionRecord.EXERCISE_TYPE_SWIMMING,
            "TABLE_TENNIS" to ExerciseSessionRecord.EXERCISE_TYPE_TABLE_TENNIS,
            // "TEAM_SPORTS" to ExerciseSessionRecord.EXERCISE_TYPE_TEAM_SPORTS,
            "TENNIS" to ExerciseSessionRecord.EXERCISE_TYPE_TENNIS,
            // "TILTING" to ExerciseSessionRecord.EXERCISE_TYPE_TILTING,
            // "VOLLEYBALL_BEACH" to ExerciseSessionRecord.EXERCISE_TYPE_VOLLEYBALL_BEACH,
            // "VOLLEYBALL_INDOOR" to ExerciseSessionRecord.EXERCISE_TYPE_VOLLEYBALL_INDOOR,
            "VOLLEYBALL" to ExerciseSessionRecord.EXERCISE_TYPE_VOLLEYBALL,
            // "WAKEBOARDING" to ExerciseSessionRecord.EXERCISE_TYPE_WAKEBOARDING,
            // "WALKING_FITNESS" to ExerciseSessionRecord.EXERCISE_TYPE_WALKING_FITNESS,
            // "WALKING_PACED" to ExerciseSessionRecord.EXERCISE_TYPE_WALKING_PACED,
            // "WALKING_NORDIC" to ExerciseSessionRecord.EXERCISE_TYPE_WALKING_NORDIC,
            // "WALKING_STROLLER" to ExerciseSessionRecord.EXERCISE_TYPE_WALKING_STROLLER,
            // "WALKING_TREADMILL" to ExerciseSessionRecord.EXERCISE_TYPE_WALKING_TREADMILL,
            "WALKING" to ExerciseSessionRecord.EXERCISE_TYPE_WALKING,
            "WATER_POLO" to ExerciseSessionRecord.EXERCISE_TYPE_WATER_POLO,
            "WEIGHTLIFTING" to ExerciseSessionRecord.EXERCISE_TYPE_WEIGHTLIFTING,
            "WHEELCHAIR" to ExerciseSessionRecord.EXERCISE_TYPE_WHEELCHAIR,
            // "WINDSURFING" to ExerciseSessionRecord.EXERCISE_TYPE_WINDSURFING,
            "YOGA" to ExerciseSessionRecord.EXERCISE_TYPE_YOGA,
            // "ZUMBA" to ExerciseSessionRecord.EXERCISE_TYPE_ZUMBA,
            // "OTHER" to ExerciseSessionRecord.EXERCISE_TYPE_OTHER,
        )
    }

    private var healthConnectClient: HealthConnectClient? = null

    private var healthConnectRequestPermissionsLauncher: ActivityResultLauncher<Set<String>>? = null

    fun initiate(activity: Activity, onHealthConnectPermissionCallback: (Set<String>) -> Unit) {
        if (isHealthConnectAvailable(activity)) {
            healthConnectClient = HealthConnectClient.getOrCreate(activity)

            val requestPermissionActivityContract = PermissionController.createRequestPermissionResultContract()
            healthConnectRequestPermissionsLauncher =
                (activity as ComponentActivity).registerForActivityResult(requestPermissionActivityContract) { granted ->
                    onHealthConnectPermissionCallback(granted)
                }
        }
    }

    fun isHealthConnectAvailable(context: Context?): Boolean {
        context?.let {
            return HealthConnectClient.getSdkStatus(it) == HealthConnectClient.SDK_AVAILABLE
        } ?: Log.e("HealthConnectService", "Context is null, cannot check availability")

        return false
    }

    suspend fun getData(call: MethodCall): List<Map<String, Any?>> = withContext(Dispatchers.IO) {
        val dataType = call.argument<String>("dataTypeKey")!!
        val startTime = Instant.ofEpochMilli(call.argument<Long>("startTimeSec")!!)
        val endTime = Instant.ofEpochMilli(call.argument<Long>("endTimeSec")!!)

        val healthConnectData = mutableListOf<Map<String, Any?>>()

        healthConnectClient?.let { healthConnectClient ->
            mapToHCType[dataType]?.let { classType ->
                val request = ReadRecordsRequest(
                    recordType = classType,
                    timeRangeFilter = TimeRangeFilter.between(startTime, endTime),
                )
                val response = healthConnectClient.readRecords(request)

                // Workout needs distance and total calories burned too
                if (dataType == WORKOUT) {
                    for (rec in response.records) {
                        val record = rec as ExerciseSessionRecord
                        val distanceRequest = healthConnectClient.readRecords(
                            ReadRecordsRequest(
                                recordType = DistanceRecord::class,
                                timeRangeFilter = TimeRangeFilter.between(
                                    record.startTime,
                                    record.endTime,
                                ),
                            ),
                        )
                        var totalDistance = 0.0
                        for (distanceRec in distanceRequest.records) {
                            totalDistance += distanceRec.distance.inMeters
                        }

                        val energyBurnedRequest = healthConnectClient.readRecords(
                            ReadRecordsRequest(
                                recordType = TotalCaloriesBurnedRecord::class,
                                timeRangeFilter = TimeRangeFilter.between(
                                    record.startTime,
                                    record.endTime,
                                ),
                            ),
                        )
                        var totalEnergyBurned = 0.0
                        for (energyBurnedRec in energyBurnedRequest.records) {
                            totalEnergyBurned += energyBurnedRec.energy.inKilocalories
                        }

                        healthConnectData.add(
                            // mapOf(
                            mapOf<String, Any?>(
                                "workoutActivityType" to (workoutTypeMapHealthConnect.filterValues { it == record.exerciseType }.keys.firstOrNull()
                                    ?: "OTHER"),
                                "totalDistance" to if (totalDistance == 0.0) null else totalDistance,
                                "totalDistanceUnit" to "METER",
                                "totalEnergyBurned" to if (totalEnergyBurned == 0.0) null else totalEnergyBurned,
                                "totalEnergyBurnedUnit" to "KILOCALORIE",
                                "unit" to "MINUTES",
                                "date_from" to rec.startTime.toEpochMilli(),
                                "date_to" to rec.endTime.toEpochMilli(),
                                "source_id" to "",
                                "source_name" to record.metadata.dataOrigin.packageName,
                            ),
                        )
                    }
                    // Filter sleep stages for requested stage
                } else if (classType == SleepStageRecord::class) {
                    for (rec in response.records) {
                        if (rec is SleepStageRecord) {
                            if (dataType == mapSleepStageToType[rec.stage]) {
                                healthConnectData.addAll(convertRecord(rec, dataType))
                            }
                        }
                    }
                } else {
                    for (rec in response.records) {
                        healthConnectData.addAll(convertRecord(rec, dataType))
                    }
                }
            }
        } ?: Log.e("HealthConnectService", "HealthConnectClient is null, cannot get data")

        return@withContext healthConnectData
    }

    suspend fun hasPermissions(call: MethodCall): Boolean = withContext(Dispatchers.IO) {
        healthConnectClient?.let {

            val args = call.arguments as HashMap<*, *>
            val types = (args["types"] as? ArrayList<*>)?.filterIsInstance<String>()!!
            val permissions = (args["permissions"] as? ArrayList<*>)?.filterIsInstance<Int>()!!

            val permList = mutableListOf<String>()
            for ((i, typeKey) in types.withIndex()) {
                val access = permissions[i]
                val dataType = mapToHCType[typeKey]!!
                if (access == 0) {
                    permList.add(
                        HealthPermission.getReadPermission(dataType),
                    )
                } else {
                    permList.addAll(
                        listOf(
                            HealthPermission.getReadPermission(dataType),
                            HealthPermission.getWritePermission(dataType),
                        ),
                    )
                }
                // Workout also needs distance and total energy burned too
                if (typeKey == WORKOUT) {
                    if (access == 0) {
                        permList.addAll(
                            listOf(
                                HealthPermission.getReadPermission(DistanceRecord::class),
                                HealthPermission.getReadPermission(TotalCaloriesBurnedRecord::class),
                            ),
                        )
                    } else {
                        permList.addAll(
                            listOf(
                                HealthPermission.getReadPermission(DistanceRecord::class),
                                HealthPermission.getReadPermission(TotalCaloriesBurnedRecord::class),
                                HealthPermission.getWritePermission(DistanceRecord::class),
                                HealthPermission.getWritePermission(TotalCaloriesBurnedRecord::class),
                            ),
                        )
                    }
                }
            }
            return@withContext it.permissionController
                .getGrantedPermissions()
                .containsAll(permList)

        } ?: Log.e("HealthConnectService", "HealthConnectClient is null, cannot check permissions")

        return@withContext false
    }

    suspend fun writeWorkoutData(call: MethodCall): Boolean = withContext(Dispatchers.IO) {
        val type = call.argument<String>("activityType")!!
        val startTime = Instant.ofEpochMilli(call.argument<Long>("startTimeSec")!!)
        val endTime = Instant.ofEpochMilli(call.argument<Long>("endTimeSec")!!)
        val totalEnergyBurned = call.argument<Int>("totalEnergyBurned")
        val totalDistance = call.argument<Int>("totalDistance")
        val workoutType = workoutTypeMapHealthConnect[type]!!

        try {
            val list = mutableListOf<Record>()
            list.add(
                ExerciseSessionRecord(
                    startTime = startTime,
                    startZoneOffset = null,
                    endTime = endTime,
                    endZoneOffset = null,
                    exerciseType = workoutType,
                    title = type,
                ),
            )
            if (totalDistance != null) {
                list.add(
                    DistanceRecord(
                        startTime = startTime,
                        startZoneOffset = null,
                        endTime = endTime,
                        endZoneOffset = null,
                        distance = Length.meters(totalDistance.toDouble()),
                    ),
                )
            }
            if (totalEnergyBurned != null) {
                list.add(
                    TotalCaloriesBurnedRecord(
                        startTime = startTime,
                        startZoneOffset = null,
                        endTime = endTime,
                        endZoneOffset = null,
                        energy = Energy.kilocalories(totalEnergyBurned.toDouble()),
                    ),
                )
            }
            healthConnectClient?.insertRecords(
                list,
            )
            Log.i("FLUTTER_HEALTH::SUCCESS", "[Health Connect] Workout was successfully added!")
            return@withContext true
        } catch (e: Exception) {
            Log.w(
                "FLUTTER_HEALTH::ERROR",
                "[Health Connect] There was an error adding the workout",
            )
            Log.w("FLUTTER_HEALTH::ERROR", e.message ?: "unknown error")
            Log.w("FLUTTER_HEALTH::ERROR", e.stackTrace.toString())
            return@withContext false
        }
    }


    suspend fun writeBloodPressure(call: MethodCall): Boolean = withContext(Dispatchers.IO) {
        val systolic = call.argument<Double>("systolic")!!
        val diastolic = call.argument<Double>("diastolic")!!
        val startTime = Instant.ofEpochMilli(call.argument<Long>("startTimeSec")!!)

        try {
            healthConnectClient?.insertRecords(
                listOf(
                    BloodPressureRecord(
                        time = startTime,
                        systolic = Pressure.millimetersOfMercury(systolic),
                        diastolic = Pressure.millimetersOfMercury(diastolic),
                        zoneOffset = null,
                    ),
                ),
            )
            Log.i(
                "FLUTTER_HEALTH::SUCCESS",
                "[Health Connect] Blood pressure was successfully added!",
            )
            return@withContext true
        } catch (e: Exception) {
            Log.w(
                "FLUTTER_HEALTH::ERROR",
                "[Health Connect] There was an error adding the blood pressure",
            )
            Log.w("FLUTTER_HEALTH::ERROR", e.message ?: "unknown error")
            Log.w("FLUTTER_HEALTH::ERROR", e.stackTrace.toString())
            return@withContext false
        }
    }

    suspend fun deleteData(call: MethodCall): Boolean = withContext(Dispatchers.IO) {
        val type = call.argument<String>("dataTypeKey")!!
        val startTime = Instant.ofEpochMilli(call.argument<Long>("startTimeSec")!!)
        val endTime = Instant.ofEpochMilli(call.argument<Long>("endTimeSec")!!)
        val classType = mapToHCType[type]!!

        try {
            healthConnectClient?.deleteRecords(
                recordType = classType,
                timeRangeFilter = TimeRangeFilter.between(startTime, endTime),
            )
            return@withContext true
        } catch (e: Exception) {
            Log.e("FLUTTER_HEALTH::ERROR", "[Health Connect] There was an error deleting the data")
            return@withContext false
        }
    }

    suspend fun writeData(call: MethodCall): Boolean = withContext(Dispatchers.IO) {
        val type = call.argument<String>("dataTypeKey")!!
        val startTime = call.argument<Long>("startTimeSec")!!
        val endTime = call.argument<Long>("endTimeSec")!!
        val value = call.argument<Double>("value")!!
        val record = when (type) {
            BODY_FAT_PERCENTAGE -> BodyFatRecord(
                time = Instant.ofEpochMilli(startTime),
                percentage = Percentage(value),
                zoneOffset = null,
            )

            HEIGHT -> HeightRecord(
                time = Instant.ofEpochMilli(startTime),
                height = Length.meters(value),
                zoneOffset = null,
            )

            WEIGHT -> WeightRecord(
                time = Instant.ofEpochMilli(startTime),
                weight = Mass.kilograms(value),
                zoneOffset = null,
            )

            STEPS -> StepsRecord(
                startTime = Instant.ofEpochMilli(startTime),
                endTime = Instant.ofEpochMilli(endTime),
                count = value.toLong(),
                startZoneOffset = null,
                endZoneOffset = null,
            )

            ACTIVE_ENERGY_BURNED -> ActiveCaloriesBurnedRecord(
                startTime = Instant.ofEpochMilli(startTime),
                endTime = Instant.ofEpochMilli(endTime),
                energy = Energy.kilocalories(value),
                startZoneOffset = null,
                endZoneOffset = null,
            )

            HEART_RATE -> HeartRateRecord(
                startTime = Instant.ofEpochMilli(startTime),
                endTime = Instant.ofEpochMilli(endTime),
                samples = listOf<HeartRateRecord.Sample>(
                    HeartRateRecord.Sample(
                        time = Instant.ofEpochMilli(startTime),
                        beatsPerMinute = value.toLong(),
                    ),
                ),
                startZoneOffset = null,
                endZoneOffset = null,
            )

            BODY_TEMPERATURE -> BodyTemperatureRecord(
                time = Instant.ofEpochMilli(startTime),
                temperature = Temperature.celsius(value),
                zoneOffset = null,
            )

            BLOOD_OXYGEN -> OxygenSaturationRecord(
                time = Instant.ofEpochMilli(startTime),
                percentage = Percentage(value),
                zoneOffset = null,
            )

            BLOOD_GLUCOSE -> BloodGlucoseRecord(
                time = Instant.ofEpochMilli(startTime),
                level = BloodGlucose.milligramsPerDeciliter(value),
                zoneOffset = null,
            )

            DISTANCE_DELTA -> DistanceRecord(
                startTime = Instant.ofEpochMilli(startTime),
                endTime = Instant.ofEpochMilli(endTime),
                distance = Length.meters(value),
                startZoneOffset = null,
                endZoneOffset = null,
            )

            WATER -> HydrationRecord(
                startTime = Instant.ofEpochMilli(startTime),
                endTime = Instant.ofEpochMilli(endTime),
                volume = Volume.liters(value),
                startZoneOffset = null,
                endZoneOffset = null,
            )

            SLEEP_ASLEEP -> SleepStageRecord(
                startTime = Instant.ofEpochMilli(startTime),
                endTime = Instant.ofEpochMilli(endTime),
                startZoneOffset = null,
                endZoneOffset = null,
                stage = SleepStageRecord.STAGE_TYPE_SLEEPING,
            )

            SLEEP_LIGHT -> SleepStageRecord(
                startTime = Instant.ofEpochMilli(startTime),
                endTime = Instant.ofEpochMilli(endTime),
                startZoneOffset = null,
                endZoneOffset = null,
                stage = SleepStageRecord.STAGE_TYPE_LIGHT,
            )

            SLEEP_DEEP -> SleepStageRecord(
                startTime = Instant.ofEpochMilli(startTime),
                endTime = Instant.ofEpochMilli(endTime),
                startZoneOffset = null,
                endZoneOffset = null,
                stage = SleepStageRecord.STAGE_TYPE_DEEP,
            )

            SLEEP_REM -> SleepStageRecord(
                startTime = Instant.ofEpochMilli(startTime),
                endTime = Instant.ofEpochMilli(endTime),
                startZoneOffset = null,
                endZoneOffset = null,
                stage = SleepStageRecord.STAGE_TYPE_REM,
            )

            SLEEP_OUT_OF_BED -> SleepStageRecord(
                startTime = Instant.ofEpochMilli(startTime),
                endTime = Instant.ofEpochMilli(endTime),
                startZoneOffset = null,
                endZoneOffset = null,
                stage = SleepStageRecord.STAGE_TYPE_OUT_OF_BED,
            )

            SLEEP_AWAKE -> SleepStageRecord(
                startTime = Instant.ofEpochMilli(startTime),
                endTime = Instant.ofEpochMilli(endTime),
                startZoneOffset = null,
                endZoneOffset = null,
                stage = SleepStageRecord.STAGE_TYPE_AWAKE,
            )


            SLEEP_SESSION -> SleepSessionRecord(
                startTime = Instant.ofEpochMilli(startTime),
                endTime = Instant.ofEpochMilli(endTime),
                startZoneOffset = null,
                endZoneOffset = null,
            )

            RESTING_HEART_RATE -> RestingHeartRateRecord(
                time = Instant.ofEpochMilli(startTime),
                beatsPerMinute = value.toLong(),
                zoneOffset = null,
            )

            BASAL_ENERGY_BURNED -> BasalMetabolicRateRecord(
                time = Instant.ofEpochMilli(startTime),
                basalMetabolicRate = Power.kilocaloriesPerDay(value),
                zoneOffset = null,
            )

            FLIGHTS_CLIMBED -> FloorsClimbedRecord(
                startTime = Instant.ofEpochMilli(startTime),
                endTime = Instant.ofEpochMilli(endTime),
                floors = value,
                startZoneOffset = null,
                endZoneOffset = null,
            )

            RESPIRATORY_RATE -> RespiratoryRateRecord(
                time = Instant.ofEpochMilli(startTime),
                rate = value,
                zoneOffset = null,
            )
            // AGGREGATE_STEP_COUNT -> StepsRecord()
            BLOOD_PRESSURE_SYSTOLIC -> throw IllegalArgumentException("You must use the [writeBloodPressure] API ")
            BLOOD_PRESSURE_DIASTOLIC -> throw IllegalArgumentException("You must use the [writeBloodPressure] API ")
            WORKOUT -> throw IllegalArgumentException("You must use the [writeWorkoutData] API ")
            else -> throw IllegalArgumentException("The type $type was not supported by the Health plugin or you must use another API ")
        }
        try {
            healthConnectClient?.insertRecords(listOf(record))
            return@withContext true
        } catch (e: Exception) {
            Log.e("XXX", "[Health Connect] There was an error adding the data", e)
            return@withContext false
        }
    }

    suspend fun getSteps(start: Long, end: Long): Long? = withContext(Dispatchers.IO) {
        try {
            val startInstant = Instant.ofEpochMilli(start)
            val endInstant = Instant.ofEpochMilli(end)
            val response = healthConnectClient?.aggregate(
                AggregateRequest(
                    metrics = setOf(StepsRecord.COUNT_TOTAL),
                    timeRangeFilter = TimeRangeFilter.between(startInstant, endInstant),
                ),
            )
            // The result may be null if no data is available in the time range.
            val stepsInInterval = response?.get(StepsRecord.COUNT_TOTAL) ?: 0L
            Log.i("FLUTTER_HEALTH::SUCCESS", "returning $stepsInInterval steps")
            return@withContext stepsInInterval
        } catch (e: Exception) {
            Log.i("FLUTTER_HEALTH::ERROR", "unable to return steps")
            return@withContext null
        }
    }

    fun requestAuthorization(call: MethodCall) {
        val args = call.arguments as java.util.HashMap<*, *>
        val types = (args["types"] as? java.util.ArrayList<*>)?.filterIsInstance<String>()!!
        val permissions = (args["permissions"] as? java.util.ArrayList<*>)?.filterIsInstance<Int>()!!

        val permList = mutableListOf<String>()
        for ((i, typeKey) in types.withIndex()) {
            val access = permissions[i]
            val dataType = mapToHCType[typeKey]!!
            if (access == 0) {
                permList.add(
                    HealthPermission.getReadPermission(dataType),
                )
            } else {
                permList.addAll(
                    listOf(
                        HealthPermission.getReadPermission(dataType),
                        HealthPermission.getWritePermission(dataType),
                    ),
                )
            }
            // Workout also needs distance and total energy burned too
            if (typeKey == WORKOUT) {
                if (access == 0) {
                    permList.addAll(
                        listOf(
                            HealthPermission.getReadPermission(DistanceRecord::class),
                            HealthPermission.getReadPermission(TotalCaloriesBurnedRecord::class),
                        ),
                    )
                } else {
                    permList.addAll(
                        listOf(
                            HealthPermission.getReadPermission(DistanceRecord::class),
                            HealthPermission.getReadPermission(TotalCaloriesBurnedRecord::class),
                            HealthPermission.getWritePermission(DistanceRecord::class),
                            HealthPermission.getWritePermission(TotalCaloriesBurnedRecord::class),
                        ),
                    )
                }
            }
        }

        healthConnectRequestPermissionsLauncher?.launch(permList.toSet())
    }

    private fun convertRecord(record: Any, dataType: String): List<Map<String, Any>> {
        val metadata = (record as Record).metadata
        when (record) {
            is WeightRecord -> return listOf(
                mapOf<String, Any>(
                    "value" to record.weight.inKilograms,
                    "date_from" to record.time.toEpochMilli(),
                    "date_to" to record.time.toEpochMilli(),
                    "source_id" to "",
                    "source_name" to metadata.dataOrigin.packageName,
                ),
            )

            is HeightRecord -> return listOf(
                mapOf<String, Any>(
                    "value" to record.height.inMeters,
                    "date_from" to record.time.toEpochMilli(),
                    "date_to" to record.time.toEpochMilli(),
                    "source_id" to "",
                    "source_name" to metadata.dataOrigin.packageName,
                ),
            )

            is BodyFatRecord -> return listOf(
                mapOf<String, Any>(
                    "value" to record.percentage.value,
                    "date_from" to record.time.toEpochMilli(),
                    "date_to" to record.time.toEpochMilli(),
                    "source_id" to "",
                    "source_name" to metadata.dataOrigin.packageName,
                ),
            )

            is StepsRecord -> return listOf(
                mapOf<String, Any>(
                    "value" to record.count,
                    "date_from" to record.startTime.toEpochMilli(),
                    "date_to" to record.endTime.toEpochMilli(),
                    "source_id" to "",
                    "source_name" to metadata.dataOrigin.packageName,
                ),
            )

            is ActiveCaloriesBurnedRecord -> return listOf(
                mapOf<String, Any>(
                    "value" to record.energy.inKilocalories,
                    "date_from" to record.startTime.toEpochMilli(),
                    "date_to" to record.endTime.toEpochMilli(),
                    "source_id" to "",
                    "source_name" to metadata.dataOrigin.packageName,
                ),
            )

            is HeartRateRecord -> return record.samples.map {
                mapOf<String, Any>(
                    "value" to it.beatsPerMinute,
                    "date_from" to it.time.toEpochMilli(),
                    "date_to" to it.time.toEpochMilli(),
                    "source_id" to "",
                    "source_name" to metadata.dataOrigin.packageName,
                )
            }

            is BodyTemperatureRecord -> return listOf(
                mapOf<String, Any>(
                    "value" to record.temperature.inCelsius,
                    "date_from" to record.time.toEpochMilli(),
                    "date_to" to record.time.toEpochMilli(),
                    "source_id" to "",
                    "source_name" to metadata.dataOrigin.packageName,
                ),
            )

            is BloodPressureRecord -> return listOf(
                mapOf<String, Any>(
                    "value" to if (dataType == BLOOD_PRESSURE_DIASTOLIC) record.diastolic.inMillimetersOfMercury else record.systolic.inMillimetersOfMercury,
                    "date_from" to record.time.toEpochMilli(),
                    "date_to" to record.time.toEpochMilli(),
                    "source_id" to "",
                    "source_name" to metadata.dataOrigin.packageName,
                ),
            )

            is OxygenSaturationRecord -> return listOf(
                mapOf<String, Any>(
                    "value" to record.percentage.value,
                    "date_from" to record.time.toEpochMilli(),
                    "date_to" to record.time.toEpochMilli(),
                    "source_id" to "",
                    "source_name" to metadata.dataOrigin.packageName,
                ),
            )

            is BloodGlucoseRecord -> return listOf(
                mapOf<String, Any>(
                    "value" to record.level.inMilligramsPerDeciliter,
                    "date_from" to record.time.toEpochMilli(),
                    "date_to" to record.time.toEpochMilli(),
                    "source_id" to "",
                    "source_name" to metadata.dataOrigin.packageName,
                ),
            )

            is DistanceRecord -> return listOf(
                mapOf<String, Any>(
                    "value" to record.distance.inMeters,
                    "date_from" to record.startTime.toEpochMilli(),
                    "date_to" to record.endTime.toEpochMilli(),
                    "source_id" to "",
                    "source_name" to metadata.dataOrigin.packageName,
                ),
            )

            is HydrationRecord -> return listOf(
                mapOf<String, Any>(
                    "value" to record.volume.inLiters,
                    "date_from" to record.startTime.toEpochMilli(),
                    "date_to" to record.endTime.toEpochMilli(),
                    "source_id" to "",
                    "source_name" to metadata.dataOrigin.packageName,
                ),
            )

            is SleepSessionRecord -> return listOf(
                mapOf<String, Any>(
                    "date_from" to record.startTime.toEpochMilli(),
                    "date_to" to record.endTime.toEpochMilli(),
                    "value" to ChronoUnit.MINUTES.between(record.startTime, record.endTime),
                    "source_id" to "",
                    "source_name" to metadata.dataOrigin.packageName,
                ),
            )

            is SleepStageRecord -> return listOf(
                mapOf<String, Any>(
                    "stage" to record.stage,
                    "value" to ChronoUnit.MINUTES.between(record.startTime, record.endTime),
                    "date_from" to record.startTime.toEpochMilli(),
                    "date_to" to record.endTime.toEpochMilli(),
                    "source_id" to "",
                    "source_name" to metadata.dataOrigin.packageName,
                ),
            )

            is RestingHeartRateRecord -> return listOf(
                mapOf<String, Any>(
                    "value" to record.beatsPerMinute,
                    "date_from" to record.time.toEpochMilli(),
                    "date_to" to record.time.toEpochMilli(),
                    "source_id" to "",
                    "source_name" to metadata.dataOrigin.packageName,
                )
            )

            is BasalMetabolicRateRecord -> return listOf(
                mapOf<String, Any>(
                    "value" to record.basalMetabolicRate.inKilocaloriesPerDay,
                    "date_from" to record.time.toEpochMilli(),
                    "date_to" to record.time.toEpochMilli(),
                    "source_id" to "",
                    "source_name" to metadata.dataOrigin.packageName,
                )
            )

            is FloorsClimbedRecord -> return listOf(
                mapOf<String, Any>(
                    "value" to record.floors,
                    "date_from" to record.startTime.toEpochMilli(),
                    "date_to" to record.endTime.toEpochMilli(),
                    "source_id" to "",
                    "source_name" to metadata.dataOrigin.packageName,
                )
            )

            is RespiratoryRateRecord -> return listOf(
                mapOf<String, Any>(
                    "value" to record.rate,
                    "date_from" to record.time.toEpochMilli(),
                    "date_to" to record.time.toEpochMilli(),
                    "source_id" to "",
                    "source_name" to metadata.dataOrigin.packageName,
                )
            )

            else -> throw IllegalArgumentException("Health data type not supported")
        }
    }

    fun clear() {
        healthConnectClient = null
    }
}