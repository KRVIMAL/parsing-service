package parser

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"time"
)

func ReadAndProcessDataPacket(buffer []byte) (interface{}, error) {
	avlData := hex.EncodeToString(buffer)
	fmt.Println(avlData, "avldata")
	var parsedData interface{}
	dataPacket := avlData[2:6]
	if dataPacket == "0200" {
		parsedMap, err := parseHexData(avlData)
		if err != nil {
			return nil, fmt.Errorf("error parsing hex data: %v", err)
		}
		parsedData = parsedMap
	} else {
		parsedMap, err := parseHexHistory(avlData)
		if err != nil {
			return nil, fmt.Errorf("error parsing hex data: %v", err)
		}
		parsedData = parsedMap
	}
	return parsedData, nil
}

func parseHexData(data string) (map[string]interface{}, error) {
	parsedData := make(map[string]interface{})

	parsedData["headerIdentifier"] = data[0:2]
	parsedData["packetType"] = data[2:6]
	parsedData["messageProperty"] = data[6:10]

	// Check if there's an extra byte between Message Property and Terminal ID
	offset := 10

	parsedData["imei"] = data[offset : offset+12]
	offset += 12

	parsedData["serialNo"] = data[offset : offset+4]
	offset += 4

	alarmFlagBits := data[offset : offset+8]
	parsedData["alarmFlagBit"] = parseAlarmFlagBits(alarmFlagBits)
	offset += 8

	statusBits := data[offset : offset+8]
	parsedData["statusBitDefinition"] = parseStatusBits(statusBits)
	offset += 8

	parsedData["latitude"] = parseGPSCoordinate(data[offset : offset+8])
	offset += 8

	parsedData["longitude"] = parseGPSCoordinate(data[offset : offset+8])
	offset += 8

	parsedData["altitude"] = convertHexToDecimal(data[offset : offset+4])
	offset += 4

	parsedData["speed"] = convertHexToDecimal(data[offset : offset+4])
	offset += 4

	parsedData["bearing"] = convertHexToDecimal(data[offset : offset+4])
	offset += 4

	dateTimeHex := data[offset : offset+12]
	dateTime, err := parseDateTime(dateTimeHex)
	if err != nil {
		return nil, fmt.Errorf("error parsing the datetime: %v", err)
	}
	parsedData["dateTime"] = dateTime
	offset += 12

	parsedMessages := []map[string]interface{}{}

	for offset < len(data)-4 {
		if offset+4 > len(data) {
			fmt.Printf("Skipping parsing at offset %d as it exceeds data length %d\n", offset, len(data))
			break
		}

		additionalMessageID := data[offset : offset+2]
		additionalMessageLength := convertHexToDecimal(data[offset+2:offset+4]) * 2
		if offset+4+additionalMessageLength > len(data) {
			fmt.Printf("Skipping parsing additional message ID %s at offset %d as it exceeds data length %d\n", additionalMessageID, offset, len(data))
			break
		}

		additionalMessageData := data[offset+4 : offset+4+additionalMessageLength]

		parsedMessage := parseAdditionalMessage(additionalMessageID, additionalMessageData)
		parsedMessages = append(parsedMessages, parsedMessage)

		offset += 4 + additionalMessageLength
	}

	parsedData["Additional Data"] = parsedMessages

	return parsedData, nil
}

// calculateBatteryPercentage calculates the battery percentage from the voltage.
func calculateBatteryPercentage(voltage int) int {
	// Convert 10mV units to mV
	millivolts := voltage * 10
	// Full charge at 4200 mV, empty at 3000 mV
	percentage := ((millivolts - 3000) * 100) / (4200 - 3000)
	if percentage > 100 {
		percentage = 100
	} else if percentage < 0 {
		percentage = 0
	}
	return percentage
}

// parseBaseStationInfo parses the Base Station Info additional message (ID 0x66)

func parseBaseStationInfo(data string) map[string]interface{} {
	parsedData := make(map[string]interface{})

	mcc := data[0:4]
	baseStations := []map[string]string{}

	lengthPerBaseStation := 18
	for i := 4; i+lengthPerBaseStation <= len(data); i += lengthPerBaseStation {
		baseStation := map[string]string{
			"RXL":    data[i : i+2],
			"MNC":    data[i+2 : i+6],
			"CELLID": data[i+6 : i+14],
			"LAC":    data[i+14 : i+18],
		}
		baseStations = append(baseStations, baseStation)
	}

	parsedData["MCC"] = mcc
	parsedData["baseStation"] = baseStations

	return parsedData
}

func hexToASCII(hexStr string) string {
	bytes, _ := hex.DecodeString(hexStr)

	return string(bytes)
}

// parseEventExtension parses the Event Extension additional message (ID 0x60)
func parseEventExtension(data string) map[string]interface{} {
	parsedData := make(map[string]interface{})

	eventID := data[0:4]
	eventType := data[4:6]
	eventContent := data[6:]

	eventDescription := getEventDescription(eventID, eventType)

	parsedData["eventId"] = eventID
	parsedData["eventType"] = eventType
	parsedData["eventContent"] = hexToASCII(eventContent)
	parsedData["eventDescription"] = eventDescription

	return parsedData
}

// getEventDescription retrieves the event description from the event ID and type
func getEventDescription(eventID string, eventType string) string {
	eventDescriptions := map[string]map[string]string{
		"0000": {"00": "Shackle closed"},
		"0001": {"00": "Shackle opened"},
		"0002": {"00": "Close shackle auto sealed"},
		"0003": {"00": "Swipe IC card to seal successfully"},
		"0004": {"00": "Swipe card to unseal successfully"},
		"0005": {"00": "BLE seal successfully"},
		"0006": {"00": "BLE unseal successfully"},
		"0007": {"00": "Platform seal successfully"},
		"0008": {"00": "Platform unseal successfully"},
		"0009": {"00": "SMS seal successfully"},
		"000A": {"00": "SMS unseal successfully"},
		"000B": {"00": "Unregistered IC card seal fails"},
		"000C": {"00": "Unregistered IC card unseal fails"},
		"0010": {"00": "Device Shell was tampered/removed"},
		"0011": {"01": "Shackle/Wire rope been cut"},
		"0012": {"00": "Battery voltage low get offline"},
		"0017": {"01": "User set low battery threshold alarm reminder"},
		"001C": {"01": "MCU communication abnormal"},
		"001D": {"00": "Fully charge"},
		"001E": {"00": "Disconnected charger"},
		"001F": {"00": "Connected charger"},
		"0020": {"00": "Device/Terminal shut down reminder"},
		"0090": {"00": "Firmware version reported event"},
	}

	if desc, ok := eventDescriptions[eventID][eventType]; ok {
		return desc
	}
	return "Unknown Event"
}

// parseAdditionalMessage parses additional messages based on their IDs.
func parseAdditionalMessage(id string, data string) map[string]interface{} {
	parsedMessage := make(map[string]interface{})

	switch id {
	case "60":
		parsedMessage = parseEventExtension(data)
	case "66":
		parsedMessage = parseBaseStationInfo(data)
	case "67":
		parsedMessage = parseDynamicPassword(data)
	case "69":
		parsedMessage = parseBatteryVoltage(data)
	case "6a":
		parsedMessage = parseNetworkCSQ(data)
	case "6b":
		parsedMessage = parseSatelliteCount(data)
	case "6c":
		parsedMessage = parseGenericMessage(data)
	case "6d":
		parsedMessage = parseGenericMessage(data)
	case "71":
		parsedMessage = parseGenericMessage(data)
	default:
		parsedMessage["Unknown Message ID"] = data
	}

	return parsedMessage
}

func parseDynamicPassword(data string) map[string]interface{} {
	return map[string]interface{}{
		"dynamicPassword": data,
	}
}

func parseBatteryVoltage(data string) map[string]interface{} {
	voltage := convertHexToDecimal(data)
	percentage := calculateBatteryPercentage(voltage)
	return map[string]interface{}{
		"batteryVoltage":    voltage,
		"batteryPercentage": percentage,
	}
}

func parseNetworkCSQ(data string) map[string]interface{} {
	csq := convertHexToDecimal(data)
	return map[string]interface{}{
		"networkCsqSignalValue": csq,
	}
}

func parseSatelliteCount(data string) map[string]interface{} {
	count := convertHexToDecimal(data)
	return map[string]interface{}{
		"satellites": count,
	}
}

func parseGenericMessage(data string) map[string]interface{} {
	return map[string]interface{}{
		"Data": hexToASCII(data),
	}
}

func parseHistoryPacket(data string) (map[string]interface{}, error) {
	packet := make(map[string]interface{})
	packet["alarmFlagBit"] = parseAlarmFlagBits(data[0:8])
	packet["statusBitsdefinition"] = parseStatusBits(data[8:16])
	packet["latitude"] = parseGPSCoordinate(data[16:24])
	packet["longitude"] = parseGPSCoordinate(data[24:32])
	packet["altitude"] = convertHexToDecimal(data[32:36])
	packet["speed"] = convertHexToDecimal(data[36:40])
	packet["bearing"] = convertHexToDecimal(data[40:44])

	// Combine the date and time hex strings into one string and parse
	dateTimeHex := data[44:56]
	dateTime, err := parseDateTime(dateTimeHex)
	if err != nil {
		return nil, fmt.Errorf("error parsing the datetime: %v", err)
	}
	packet["dateTime"] = dateTime

	// Parse additional data
	offset := 56
	additionalData := []map[string]interface{}{}
	for offset+4 <= len(data) {
		additionalMessageID := data[offset : offset+2]
		additionalMessageLength := convertHexToDecimal(data[offset+2:offset+4]) * 2
		if offset+4+additionalMessageLength > len(data) {
			fmt.Printf("Skipping parsing additional message ID %s at offset %d as it exceeds data length %d\n", additionalMessageID, offset, len(data))
			break
		}

		additionalMessageData := data[offset+4 : offset+4+additionalMessageLength]
		parsedMessage := parseAdditionalMessage(additionalMessageID, additionalMessageData)
		additionalData = append(additionalData, parsedMessage)

		offset += 4 + additionalMessageLength
	}

	packet["Additional Data"] = additionalData

	return packet, nil
}

// Parse the hex data into separate history packets
func parseHexHistory(data string) ([]map[string]interface{}, error) {
	var packets []map[string]interface{}

	// Parse common information
	commonInfo := make(map[string]interface{})
	commonInfo["headerIdentifier"] = data[0:2]
	commonInfo["packetType"] = data[2:6]
	messagePropertyHex := data[6:10]
	commonInfo["messageProperty"] = messagePropertyHex
	commonInfo["imei"] = data[10:22]
	commonInfo["serialNo"] = data[22:26]

	// Get the total length from the message property
	totalLength := convertHexToDecimal(messagePropertyHex) * 2
	offset := 26

	for totalLength > 0 && offset < len(data)-4 {
		if offset+2 > len(data) {
			fmt.Printf("Skipping parsing packet length at offset %d as it exceeds data length %d\n", offset, len(data))
			break
		}

		// Skip delimiters if present
		packetLengthHex := data[offset : offset+2]
		packetLength := convertHexToDecimal(packetLengthHex) * 2
		offset += 2

		if offset+packetLength > len(data) {
			fmt.Printf("Skipping parsing packet at offset %d as it exceeds data length %d\n", offset, len(data))
			break
		}

		packetData := data[offset : offset+packetLength]
		packet, err := parseHistoryPacket(packetData)
		if err != nil {
			fmt.Printf("Error parsing history packet at offset %d: %v\n", offset, err)
			break
		}
		packet["headerIdentifier"] = commonInfo["headerIdentifier"]
		packet["packetType"] = commonInfo["packetType"]
		packet["messageProperty"] = commonInfo["messageProperty"]
		packet["Serial Number"] = commonInfo["Serial Number"]
		packet["imei"] = commonInfo["imei"]

		packets = append(packets, packet)

		offset += packetLength
		totalLength -= packetLength
	}

	return packets, nil
}

// parseAlarmFlagBits parses the alarm flag bits according to the provided protocol documentation.
func parseAlarmFlagBits(alarmFlagBits string) map[string]bool {
	parsedBits := make(map[string]bool)
	flags, _ := strconv.ParseUint(alarmFlagBits, 16, 64)

	binaryString := fmt.Sprintf("%032b", flags)
	// Reverse the binary string to start parsing from the right-hand side
	reversedBinStr := reverseString(binaryString)

	bitDefinitions := []string{
		"shackleDamaged",
		"shellTampered",
		"lowBattery",
		"mcuCommAbnormal",
		"shackleWireCut",
		"gpsAntennaOpen",
		"gpsAntennaShort",
		"gpsAntennaShort",
		"motorStuckUnseal",
		"overSpeed",
		"timeoutParking",
		"gnssFailure",
		"mainPowerFailure",
	}

	for i, definition := range bitDefinitions {
		parsedBits[definition] = reversedBinStr[i] == '1'
	}

	return parsedBits
}

// parseStatusBits parses the status bits according to the provided protocol documentation.
func parseStatusBits(statusBits string) map[string]interface{} {
	flags, _ := strconv.ParseUint(statusBits, 16, 64)
	binaryString := fmt.Sprintf("%032b", flags)
	reversedBinStr := reverseString(binaryString)

	status := map[string]interface{}{}

	// Bit 0
	status["ignitionOn"] = reversedBinStr[0] == '1'

	// Bit 1
	status["gps"] = reversedBinStr[1] == '1'

	// Bit 2
	if reversedBinStr[2] == '1' {
		status["latitudeUnit"] = "South Latitude"
	} else {
		status["latitudeUnit"] = "North Latitude"
	}

	// Bit 3
	if reversedBinStr[3] == '1' {
		status["longitudeUnit"] = "West Longitude"
	} else {
		status["longitudeUnit"] = "East Longitude"
	}

	// Bit 4
	status["connection"] = reversedBinStr[4] == '1'

	// Bit 5
	status["gpsOn"] = reversedBinStr[5] == '1'

	// Bit 6
	status["motionState"] = reversedBinStr[6] == '1'

	// Bits 7-13 are reserved

	// Bit 14
	status["sealOpen"] = reversedBinStr[14] == '1'

	// Bit 15
	status["shackleOpen"] = reversedBinStr[15] == '1'

	// Bits 16-17
	chargeStatus := reversedBinStr[16:18]
	switch chargeStatus {
	case "00":
		status["chargeStatus"] = "Non charge"
	case "01":
		status["chargeStatus"] = "In-charging"
	case "10":
		status["chargeStatus"] = "Full charge"
	}

	// Bits 18-19
	netStatus := reversedBinStr[18:20]
	switch netStatus {
	case "00":
		status["network"] = "2G"
	case "01":
		status["network"] = "3G"
	case "10":
		status["network"] = "4G"
	}

	// Bits 20-21
	simStatus := reversedBinStr[20:22]
	switch simStatus {
	case "00":
		status["activeSim"] = "Default"
	case "01":
		status["activeSim"] = "SIM card 1"
	case "10":
		status["activeSim"] = "SIM card 2"
	}

	// Bits 22-23
	connectStatus := reversedBinStr[22:24]
	switch connectStatus {
	case "00":
		status["connectionType"] = "Non"
	case "01":
		status["connectionType"] = "WIFI"
	case "10":
		status["connectionType"] = "RJ45"
	}

	return status
}

func reverseString(s string) string {
	runes := []rune(s)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes)
}

// parseGPSCoordinate parses a GPS coordinate from a hexadecimal string to a decimal value.
func parseGPSCoordinate(hexString string) float64 {
	value, _ := strconv.ParseUint(hexString, 16, 64)
	return float64(value) / 1000000.0
}

// convertHexToDecimal converts a hexadecimal string to a decimal integer.
func convertHexToDecimal(hexString string) int {
	value, _ := strconv.ParseUint(hexString, 16, 64)
	return int(value)
}

func parseDateTime(hexTimestamp string) (string, error) {
	if len(hexTimestamp) != 12 {
		return "", fmt.Errorf("invalid hex timestamp length")
	}

	year, err := strconv.ParseInt(hexTimestamp[0:2], 10, 64)
	if err != nil {
		return "", fmt.Errorf("error parsing year: %v", err)
	}
	year += 2000 // Assuming all dates are in the 21st century

	month, err := strconv.ParseInt(hexTimestamp[2:4], 10, 64)
	if err != nil {
		return "", fmt.Errorf("error parsing month: %v", err)
	}

	day, err := strconv.ParseInt(hexTimestamp[4:6], 10, 64)
	if err != nil {
		return "", fmt.Errorf("error parsing day: %v", err)
	}

	hour, err := strconv.ParseInt(hexTimestamp[6:8], 10, 64)
	if err != nil {
		return "", fmt.Errorf("error parsing hour: %v", err)
	}

	minute, err := strconv.ParseInt(hexTimestamp[8:10], 10, 64)
	if err != nil {
		return "", fmt.Errorf("error parsing minute: %v", err)
	}

	second, err := strconv.ParseInt(hexTimestamp[10:12], 10, 64)
	if err != nil {
		return "", fmt.Errorf("error parsing second: %v", err)
	}
	// Create a time.Time object in UTC
	t := time.Date(int(year), time.Month(month), int(day), int(hour), int(minute), int(second), 0, time.UTC)

	// Format the time.Time object to an ISO 8601 string
	isoTimestamp := t.Format(time.RFC3339)

	return isoTimestamp, nil
}
