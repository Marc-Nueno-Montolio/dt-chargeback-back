from sqlalchemy import Column, Integer, String, Boolean, Float, DateTime, Table, ForeignKey
from sqlalchemy.orm import relationship
from datetime import datetime
from database import Base

# Many-to-many association tables
host_dgs = Table(
    'host_dgs',
    Base.metadata,
    Column('host_id', Integer, ForeignKey('hosts.id')),
    Column('dg_id', Integer, ForeignKey('dgs.id'))
)

host_is = Table(
    'host_is',
    Base.metadata,
    Column('host_id', Integer, ForeignKey('hosts.id')),
    Column('is_id', Integer, ForeignKey('information_systems.id'))
)

application_dgs = Table(
    'application_dgs',
    Base.metadata,
    Column('application_id', Integer, ForeignKey('applications.id')),
    Column('dg_id', Integer, ForeignKey('dgs.id'))
)

application_is = Table(
    'application_is',
    Base.metadata,
    Column('application_id', Integer, ForeignKey('applications.id')),
    Column('is_id', Integer, ForeignKey('information_systems.id'))
)

synthetic_dgs = Table(
    'synthetic_dgs',
    Base.metadata,
    Column('synthetic_id', Integer, ForeignKey('synthetics.id')),
    Column('dg_id', Integer, ForeignKey('dgs.id'))
)

synthetic_is = Table(
    'synthetic_is',
    Base.metadata,
    Column('synthetic_id', Integer, ForeignKey('synthetics.id')),
    Column('is_id', Integer, ForeignKey('information_systems.id'))
)

class Host(Base):
    """
    Represents a host entity in the database.
    """
    __tablename__ = "hosts"
    id = Column(Integer, primary_key=True, index=True)
    dt_id = Column(String, unique=True, index=True)
    name = Column(String)
    managed = Column(Boolean, default=False)
    memory_gb = Column(Float)
    tags = Column(String)  # JSON string
    state = Column(String)
    monitoring_mode = Column(String)
    last_updated = Column(DateTime, default=datetime.utcnow)

    # Many-to-many relationships
    dgs = relationship("DG", secondary=host_dgs, back_populates="hosts")
    information_systems = relationship("IS", secondary=host_is, back_populates="hosts")

class DG(Base):
    """
    Represents a DG (Dynamic Group) entity in the database.
    """
    __tablename__ = "dgs"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    last_updated = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    hosts = relationship("Host", secondary=host_dgs, back_populates="dgs")
    applications = relationship("Application", secondary=application_dgs, back_populates="dgs")
    synthetics = relationship("Synthetic", secondary=synthetic_dgs, back_populates="dgs")
    information_systems = relationship("IS", back_populates="dg")

class IS(Base):
    """
    Represents an Information System entity in the database.
    """
    __tablename__ = "information_systems"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    dg_id = Column(Integer, ForeignKey('dgs.id'))
    last_updated = Column(DateTime, default=datetime.utcnow)
    managed = Column(Boolean, default=False)
    
    # Relationships
    dg = relationship("DG", back_populates="information_systems")
    hosts = relationship("Host", secondary=host_is, back_populates="information_systems")
    applications = relationship("Application", secondary=application_is, back_populates="information_systems")
    synthetics = relationship("Synthetic", secondary=synthetic_is, back_populates="information_systems")

class Application(Base):
    """
    Represents an Application entity in the database.
    """
    __tablename__ = "applications"
    id = Column(Integer, primary_key=True, index=True)
    dt_id = Column(String, index=True)
    name = Column(String, index=True)
    type = Column(String)
    tags = Column(String)  # JSON string
    last_updated = Column(DateTime, default=datetime.utcnow)

    # Relationships
    dgs = relationship("DG", secondary=application_dgs, back_populates="applications")
    information_systems = relationship("IS", secondary=application_is, back_populates="applications")

class Synthetic(Base):
    """
    Represents a Synthetic entity in the database.
    """
    __tablename__ = "synthetics"
    id = Column(Integer, primary_key=True, index=True)
    dt_id = Column(String, index=True)
    name = Column(String, index=True)
    type = Column(String)
    frequency = Column(Integer)
    tags = Column(String)  # JSON string
    last_updated = Column(DateTime, default=datetime.utcnow)
    http_type_tag = Column(String)
    is_custom_monitor = Column(Boolean, default=False)

    # Relationships
    dgs = relationship("DG", secondary=synthetic_dgs, back_populates="synthetics")
    information_systems = relationship("IS", secondary=synthetic_is, back_populates="synthetics")

class Report(Base):
    """
    Represents a Report entity in the database.
    """
    __tablename__ = "reports"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    from_date = Column(DateTime)
    to_date = Column(DateTime)
    last_updated = Column(DateTime, default=datetime.utcnow)
    status = Column(String)
    data = Column(String)
