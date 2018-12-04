package com.cdd.bin

import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder
import com.amazonaws.services.securitytoken.model.GetSessionTokenRequest

/**
  * Application to print shell command to set AWS credentials environment
  */
object AwsEnvironment extends App {

  if (args.length == 0 || args.length > 2) {
    println(s"Usage ${getClass.getName} <token-code> [serial_number]")
  } else {
    val token = args(0)
    val serialNumber = if (args.length == 2) args(1) else "arn:aws:iam::123456789012:mfa/user"
    val client = AWSSecurityTokenServiceClientBuilder.defaultClient()
    val tokenRequest = (new GetSessionTokenRequest).withDurationSeconds(129600).withSerialNumber(serialNumber).withTokenCode(token)
    val credentials = client.getSessionToken(tokenRequest).getCredentials

    println(s"export AWS_ACCESS_KEY_ID=${credentials.getAccessKeyId}")
    println(s"export AWS_SECRET_ACCESS_KEY=${credentials.getSecretAccessKey}")
    println(s"export AWS_SESSION_TOKEN=${credentials.getSessionToken}")
    println(s"export AWS_SECURITY_TOKEN=${credentials.getSessionToken}")

  }
}
