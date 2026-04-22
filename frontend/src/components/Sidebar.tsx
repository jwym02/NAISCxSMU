import { NavLink } from "react-router-dom";

function GridIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor">
      <rect x="1" y="1" width="6" height="6" rx="1" />
      <rect x="9" y="1" width="6" height="6" rx="1" />
      <rect x="1" y="9" width="6" height="6" rx="1" />
      <rect x="9" y="9" width="6" height="6" rx="1" />
    </svg>
  );
}

export function Sidebar() {
  return (
    <aside className="sidebar">
      <div className="sidebar-logo">
        <div className="logo-icon">
          <GridIcon />
        </div>
        <div>
          <div className="logo-name">LOGPIPE</div>
          <div className="logo-version">v0.1 · LOCAL</div>
        </div>
      </div>

      <nav className="sidebar-nav">
        <div className="nav-section-label">PAGES</div>
        <NavLink
          to="/"
          end
          className={({ isActive }) => `nav-item${isActive ? " active" : ""}`}
        >
          Dashboard
        </NavLink>
        <NavLink
          to="/analytics"
          className={({ isActive }) => `nav-item${isActive ? " active" : ""}`}
        >
          Analytics
        </NavLink>
        <span className="nav-item disabled">Schema</span>
      </nav>
    </aside>
  );
}
