# CSU Data HUB deployment guide
#### This guide has been put together in support of data fabric and data mesh deployments within Azure Secret.
#### What is Data Fabric?
---Insert Data Fabric definition

#### What is Data Mesh?
---Insert Data mesh definition

#### How does the CSU Data Hub model fall into this construct?
---Describe architecture and components comrpised of Data Hub
BLUF: Our strategy is geared\focused within IL6 enclaves but is similar to what is also being developed\mapped out in IL4\5 respectively - we have developed a "Hub" construct that supports the data fabric/data mesh methodology of being able to move data from a variety of different domains, into a centralized location with the capability to tag and govern as data is at rest\coming in at the ingest point. Leveraging a series of built in functionality with AD, we can use RBAC to restrict who has access to what data and segregate out data sets based on requirements but the crux of it all is ensuring the foundation is established to support a variety of use cases. 

Azure Synapse Analytics at the core is the fundamental solution to support such use cases whether we are talking about ingesting, transforming, moving, storing data across the directorates and everything else can be integrated in to support as we move out. 

#### Zero trust
---Describe Zero trust methodology behind CSU Data Hub

#### CSU Data Hub Architecture
---Describe architecture and define pipeline integration

#### Deployment Guide
---Outline deployment of Apache Atlas ---Outline deployment of Synapse and Synapse Pipeline in support of codebase ---Create trigger based on new files being dropped in ADLS ---APIM integration
