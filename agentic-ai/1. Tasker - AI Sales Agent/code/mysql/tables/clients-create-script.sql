SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [RE_AGENTS].[clients](
	[client_id] [int] IDENTITY(1,1) NOT NULL,
	[project_id] [bigint] NULL,
	[client_name] [varchar](max) NULL,
	[investment_type] [varchar](max) NULL,
	[preferred_contact_method] [varchar](max) NULL,
	[room_count] [int] NULL,
	[client_email] [varchar](max) NULL,
	[client_phone_number] [varchar](max) NULL,
	[creation_date] [datetime] NULL,
	[assigned_advisor_id] [bigint] NULL,
	[company_id] [bigint] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
ALTER TABLE [RE_AGENTS].[clients] ADD PRIMARY KEY CLUSTERED 
(
	[client_id] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
ALTER TABLE [RE_AGENTS].[clients] ADD  DEFAULT (getdate()) FOR [creation_date]
GO
