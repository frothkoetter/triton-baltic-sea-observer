import { useEffect } from "react";
import { useMap } from "react-leaflet";

/** Leaflet often initializes before the panel has final width (grid/tabs). */
export function MapInvalidateSize() {
  const map = useMap();
  useEffect(() => {
    const invalidate = () => map.invalidateSize({ animate: false });
    invalidate();
    const t1 = window.setTimeout(invalidate, 0);
    const t2 = window.setTimeout(invalidate, 120);
    const parent = map.getContainer().parentElement;
    const ro = parent ? new ResizeObserver(invalidate) : null;
    if (parent && ro) ro.observe(parent);
    window.addEventListener("resize", invalidate);
    return () => {
      window.clearTimeout(t1);
      window.clearTimeout(t2);
      ro?.disconnect();
      window.removeEventListener("resize", invalidate);
    };
  }, [map]);
  return null;
}