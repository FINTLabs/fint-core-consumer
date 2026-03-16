package no.fintlabs.consumer.kafka.entity

object KeyParser {

    private const val KEY_DELIMITER = "\u001F"

    fun extractIdentifier(key: String): String =
        key.substringAfter(KEY_DELIMITER, missingDelimiterValue = key)

}
