This exercise was created using an online version of DuckDB and assumes the data is already formatted after Part1.

The dataset used is also in this repository.

The idea is to have a sequence of queries that will clean the data and have the information ready for analytics or other use cases.

What We're Looking For
● How do you approach sessionization?
● What attributes and metrics do you choose to calculate?
● How do you handle attribution in a real e-commerce context?
● Does your output reconcile with your inputs?
● Is your code maintainable and your architecture scalable?

1. How do you approach sessionization?
I'm using a 15-minute window between events, logic similar that systems such as Amplitude use to start and end sessions. If the sequence of events has an interval of > 15 minutes, I create a new session using a simple uuid.
The dataset also does not have event_ids, so I'm also creating one for the entire dataset using uuids as well.

2. What attributes and metrics do you choose to calculate?
I extracted all possible data from the URL and categorized them into specific channels for affilliates and brands (google, bing and fb). The UTM is not reliable since it is encoded and I don't have access to what each means, and the referrer is also not reliable since it does not have data during a specific period.
I also flagged the conversion events, but I did not parse the event_properties such as order_number or revenue from there, as I believe this data would reliably come from internal systems instead.

3. How do you handle attribution in a real e-commerce context?
Given the 7d window mentioned, I'm checking for the first and last channel in the past 7 days. The actual conversion events do not have data from where they came from, so we need to get it from the first events in that session and store them sequentially.
For the multi-attributes part, we'd have to align with the business how to split, and if only first and last would be enough, or a ratio amongst all identified channels until a conversion.

4. Does your output reconcile with your inputs?
There are no tests in the repo, but my recon is to compare number of rows after each step to ensure we're not losing data. More data quality tests would be necessary to ensure proper solution.

5. Is your code maintainable and your architecture scalable?
The architecture is scalable and the code is simple and maintainable. However, we would need to add an automated extractor of URL properties and parse them, as I did so manually in the exercise for simplicity.



