SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [RE_AGENTS].[properties](
	[property_id] [bigint] IDENTITY(1,1) NOT NULL,
	[project_id] [bigint] NULL,
	[property_type] [varchar](255) NULL,
	[property_identifier] [varchar](255) NULL,
	[property_availability] [bit] NULL,
	[property_price_usd] [float] NULL,
	[property_square_meters] [float] NULL,
	[property_parking_spaces] [float] NULL,
	[creation_date] [datetime] NULL,
	[property_floor_plan_url] [varchar](255) NULL,
	[property_level] [varchar](255) NULL,
	[reserved_agent_id] [bigint] NULL,
	[reservation_date] [datetime] NULL,
	[division_type] [varchar](255) NULL,
	[division_identifier] [varchar](255) NULL,
	[enterprise_id] [bigint] NULL
) ON [PRIMARY]
GO
ALTER TABLE [RE_AGENTS].[properties] ADD PRIMARY KEY CLUSTERED 
(
	[property_id] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
ALTER TABLE [RE_AGENTS].[properties] ADD  DEFAULT (getdate()) FOR [creation_date]
GO
