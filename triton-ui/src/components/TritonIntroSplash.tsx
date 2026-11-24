import { useCallback, useEffect, useRef, useState, type ReactNode } from "react";

const INTRO_SEEN_KEY = "triton_intro_seen_v1";

export function introAlreadySeen(): boolean {
  try {
    return sessionStorage.getItem(INTRO_SEEN_KEY) === "1";
  } catch {
    return false;
  }
}

export function markIntroSeen(): void {
  try {
    sessionStorage.setItem(INTRO_SEEN_KEY, "1");
  } catch {
    /* ignore */
  }
}

type Props = {
  onEnter: () => void;
};

export function TritonIntroSplash({ onEnter }: Props) {
  const stageRef = useRef<HTMLDivElement | null>(null);
  const [tilt, setTilt] = useState({ rx: 8, ry: -12 });
  const reducedMotion =
    typeof window !== "undefined" && window.matchMedia("(prefers-reduced-motion: reduce)").matches;

  const handleEnter = useCallback(() => {
    markIntroSeen();
    onEnter();
  }, [onEnter]);

  useEffect(() => {
    if (reducedMotion) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Enter" || e.key === " ") {
        e.preventDefault();
        handleEnter();
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [handleEnter, reducedMotion]);

  const onPointerMove = (e: React.PointerEvent) => {
    const el = stageRef.current;
    if (!el || reducedMotion) return;
    const r = el.getBoundingClientRect();
    const px = (e.clientX - r.left) / r.width - 0.5;
    const py = (e.clientY - r.top) / r.height - 0.5;
    setTilt({ rx: 8 - py * 14, ry: -12 + px * 22 });
  };

  const onPointerLeave = () => {
    if (!reducedMotion) setTilt({ rx: 8, ry: -12 });
  };

  return (
    <div className="triton-intro-overlay" role="dialog" aria-modal aria-labelledby="triton-intro-title">
      <div className="triton-intro-backdrop" aria-hidden />
      <motionGrid />

      <div
        ref={stageRef}
        className="triton-intro-stage"
        onPointerMove={onPointerMove}
        onPointerLeave={onPointerLeave}
      >
        <div
          className="triton-intro-scene"
          style={{
            transform: `rotateX(${tilt.rx}deg) rotateY(${tilt.ry}deg)`,
          }}
        >
          <div className="triton-intro-orbit" aria-hidden />
          <div className="triton-intro-orbit triton-intro-orbit--inner" aria-hidden />
          <motionPlate reducedMotion={reducedMotion}>
            <img
              src="/images/triton-showcase-3d.jpg"
              alt="TRITON Show Case — Baltic Sea Observing Platform"
              className="triton-intro-hero"
              width={480}
              height={480}
              draggable={false}
            />
          </motionPlate>
          <motionShadow reducedMotion={reducedMotion} tilt={tilt} />
        </div>

        <motionTitle id="triton-intro-title" reducedMotion={reducedMotion} />
      </div>

      <div className="triton-intro-actions">
        <button type="button" className="btn-primary triton-intro-enter" onClick={handleEnter}>
          Plattform öffnen
        </button>
        <p className="text-muted triton-intro-hint">
          NATO UNCLASSIFIED // TRAINING · Mausbewegung kippt die 3D-Ansicht
        </p>
      </div>
    </div>
  );
}

function motionGrid() {
  return <div className="triton-intro-grid" aria-hidden />;
}

function motionPlate({
  children,
  reducedMotion,
}: {
  children: ReactNode;
  reducedMotion: boolean;
}) {
  return (
    <div className={`triton-intro-plate ${reducedMotion ? "" : "triton-intro-float"}`}>
      <div className="triton-intro-glow" aria-hidden />
      {children}
    </div>
  );
}

function motionShadow({
  reducedMotion,
  tilt,
}: {
  reducedMotion: boolean;
  tilt: { rx: number; ry: number };
}) {
  const skew = reducedMotion ? 0 : tilt.ry * 0.4;
  return (
    <div
      className="triton-intro-ground-shadow"
      style={{ transform: `translateZ(-80px) rotateZ(${skew}deg) scale(0.85)` }}
      aria-hidden
    />
  );
}

function motionTitle({ id, reducedMotion }: { id: string; reducedMotion: boolean }) {
  return (
    <motionText id={id} className={`triton-intro-copy ${reducedMotion ? "" : "triton-intro-fade-in"}`}>
      <span className="triton-intro-kicker">BALTIC SEA OBSERVING PLATFORM</span>
      <span className="triton-intro-brand">TRITON SHOW CASE</span>
    </motionText>
  );
}

function motionText({
  id,
  className,
  children,
}: {
  id: string;
  className: string;
  children: ReactNode;
}) {
  return (
    <div id={id} className={className}>
      {children}
    </div>
  );
}