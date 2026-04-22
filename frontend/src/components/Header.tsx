interface HeaderProps {
  title: string;
  subtitle: string;
  jobsToday?: number;
}

export function Header({ title, subtitle, jobsToday }: HeaderProps) {
  const date = new Date().toLocaleDateString("en-US", {
    month: "long",
    day: "numeric",
    year: "numeric",
  });

  return (
    <header className="page-header">
      <div>
        <h1 className="page-title">{title}</h1>
        <p className="page-subtitle">{subtitle}</p>
      </div>
      <div className="header-badges">
        <span className="badge">{date}</span>
        {jobsToday !== undefined && (
          <span className="badge">{jobsToday} jobs today</span>
        )}
      </div>
    </header>
  );
}
