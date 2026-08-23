-- =============================================================================
-- DIMENSION TABLE: dim_branch
-- =============================================================================
-- Type: Static Dimension
-- Purpose: Store branch information for geographic analysis
-- =============================================================================

CREATE TABLE dw.dim_branch (
    branch_sk           SERIAL PRIMARY KEY,
    branch_code         VARCHAR(10) NOT NULL,
    branch_name         VARCHAR(100),
    branch_type         VARCHAR(20),  -- MAIN, REGIONAL, SUB_BRANCH
    city                VARCHAR(50),
    state               VARCHAR(50),
    region              VARCHAR(50),  -- NORTH, CENTRAL, SOUTH
    country             VARCHAR(50) DEFAULT 'VIETNAM',
    is_active           BOOLEAN DEFAULT TRUE,
    source_system       VARCHAR(50),
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX idx_dim_branch_code ON dw.dim_branch(branch_code);
CREATE INDEX idx_dim_branch_region ON dw.dim_branch(region);

-- Sample Data
INSERT INTO dw.dim_branch (branch_code, branch_name, branch_type, city, state, region, source_system)
VALUES
('BR001', 'Ho Chi Minh Main', 'MAIN', 'Ho Chi Minh', 'HCM', 'SOUTH', 'CORE_BANKING'),
('BR002', 'Hanoi Branch', 'REGIONAL', 'Ha Noi', 'HN', 'NORTH', 'CORE_BANKING'),
('BR003', 'Da Nang Branch', 'REGIONAL', 'Da Nang', 'DN', 'CENTRAL', 'CORE_BANKING'),
('BR004', 'Can Tho Branch', 'SUB_BRANCH', 'Can Tho', 'CT', 'SOUTH', 'CORE_BANKING'),
('BR005', 'Hai Phong Branch', 'REGIONAL', 'Hai Phong', 'HP', 'NORTH', 'CORE_BANKING'),
('BR006', 'Hue Branch', 'SUB_BRANCH', 'Hue', 'TH', 'CENTRAL', 'CORE_BANKING'),
('BR007', 'Nha Trang Branch', 'SUB_BRANCH', 'Nha Trang', 'KH', 'CENTRAL', 'CORE_BANKING'),
('BR008', 'Vung Tau Branch', 'SUB_BRANCH', 'Vung Tau', 'BR', 'SOUTH', 'CORE_BANKING');

COMMENT ON TABLE dw.dim_branch IS 'Branch dimension for geographic analysis';
