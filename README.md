# conference-dynamodb
Sample code for the Nexosis conference presentation "Dumpster Fire to Lit: Time-Series Data in Amazon DynamoDB"

Used to create, drop, and scale tables for the conference presentation, as well as read and write documents in various ways.

For usage:
```
dotnet Nexosis.Conference.DynamoDB.dll --help
```

In order to use the sample, you will need to set environment variables for your AWS access key ID and secret access key, as described in the link:
http://docs.aws.amazon.com/cli/latest/userguide/cli-environment.html
