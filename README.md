# CSU Data HUB deployment guide
#### This guide has been put together in support of data fabric and data mesh deployments within Azure Secret.
#### What is Data Fabric?
Data Fabric means that all of your organization's data is connected through a single, unified system irreseptive of where it resides (on-premises or cloud). This system serves as an abstraction layer between users and underlying technologies with no limitations on where you can move your data. This abstraction layer can be used by your teams to access, analyze and manage your data in various formats.

#### What is Data Mesh?
A data mesh centers around organizing your company's data into domains rather than around specific applications. Opposite of a fabric, which can handle multiple applications, a data mesh is only concerned with organizing the results of each application into domains, often built on microservices architecture and exposes data through APIs that can be consumed by other applications.

#### How does the CSU Data Hub model fall into this construct?
The strategy is geared\focused within IL6 enclaves but is similar to what is also being developed\mapped out in IL4\5 respectively - we have developed a "Hub" construct that supports the data fabric/data mesh methodology of being able to move data from a variety of different domains, into a centralized location with the capability to tag and govern as data is at rest\coming in at the ingest point. Leveraging a series of built in functionality with AD, we can use RBAC to restrict who has access to what data and segregate out data sets based on requirements but the crux of it all is ensuring the foundation is established to support a variety of use cases. 

#### Zero trust
---Describe Zero trust methodology behind CSU Data Hub

#### CSU Data Hub Architecture
**Azure Synapse Analytics** - at the core is the fundamental solution to support such use cases whether we are talking about ingesting, transforming, moving, storing data across the directorates and everything else can be integrated in to support as we move out.

**Azure Data Lake Storage Gen2** - provides file system semantics, file-level security, and scale. Azure Data Lake Storage is a cloud-based, enterprise data lake solution. It is engineered to store massive amounts of data in any format, and to faciliate big data anlytical workloads.

**Apache Atlas** - Atlas is a scalable and extensible set of core foundational governance services - enabling enterprises to effectively meet their compliance requirements within Hadoop and allows integration with the whole enterprise data ecosystem. Apache Atlas provides open metadata management and governance capabilities for organizations to build a catalog of their data assets, classify and goven these assets and provide collaboration capabilities around these data assets for data scientists, analysts and the data governance team.

#### Deployment Guide
---Outline deployment of Apache Atlas ---Outline deployment of Synapse and Synapse Pipeline in support of codebase ---Create trigger based on new files being dropped in ADLS ---APIM integration
