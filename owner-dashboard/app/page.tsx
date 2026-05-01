import fs from "node:fs/promises"
import path from "node:path"

import dashboardData from "@/app/data/dashboard-data.json"
import { DashboardView } from "@/components/dashboard-view"

type RestaurantRow = {
  id: string
  name: string
  cuisine: string
  location: string
  rating: number | null
  hasOnlineOrdering: boolean | null
  hasPickup: boolean | null
  hasCatering: boolean | null
  description: string
  positiveReviewPoints: string
  negativeReviewPoints: string
  firstOrderPlatform: string
  allOrderPlatforms: string
  fitScore: number | null
}

function parseCsvLine(line: string): string[] {
  const fields: string[] = []
  let current = ""
  let inQuotes = false
  for (let i = 0; i < line.length; i += 1) {
    const char = line[i]
    if (char === '"') {
      const next = line[i + 1]
      if (inQuotes && next === '"') {
        current += '"'
        i += 1
      } else {
        inQuotes = !inQuotes
      }
      continue
    }
    if (char === "," && !inQuotes) {
      fields.push(current)
      current = ""
      continue
    }
    current += char
  }
  fields.push(current)
  return fields.map((x) => x.trim())
}

async function loadRestaurants(): Promise<RestaurantRow[]> {
  const projectRoot = path.resolve(process.cwd(), "..")
  const restaurantCsvPath = path.join(projectRoot, "restaurant.csv")
  const ratingCsvPath = path.join(projectRoot, "restaurants/data/restaurant-places-review-summary.csv")
  const capabilityCsvPath = path.join(projectRoot, "restaurants/data/restaurant-capabilities-summary.csv")
  const descriptionCsvPath = path.join(projectRoot, "restaurants/data/restaurant-descriptions.csv")
  const orderSetupCsvPath = path.join(projectRoot, "restaurants/data/restaurant-order-platform-minimal.csv")
  const fitRankingCsvPath = path.join(projectRoot, "restaurants/data/restaurant-fit-ranking.csv")

  const [restaurantCsv, ratingCsv, capabilityCsv, descriptionCsv, orderSetupCsv, fitRankingCsv] = await Promise.all([
    fs.readFile(restaurantCsvPath, "utf-8"),
    fs.readFile(ratingCsvPath, "utf-8"),
    fs.readFile(capabilityCsvPath, "utf-8"),
    fs.readFile(descriptionCsvPath, "utf-8"),
    fs.readFile(orderSetupCsvPath, "utf-8"),
    fs.readFile(fitRankingCsvPath, "utf-8"),
  ])

  const ratings = new Map<string, number | null>()
  const reviewPoints = new Map<string, { positive: string; negative: string }>()
  const ratingLines = ratingCsv.split(/\r?\n/).filter(Boolean)
  const ratingHeader = parseCsvLine(ratingLines[0] ?? "")
  const ratingIdIdx = ratingHeader.indexOf("restaurant_id")
  const ratingIdx = ratingHeader.indexOf("rating")
  const positivePointsIdx = ratingHeader.indexOf("positive_reviews_points")
  const negativePointsIdx = ratingHeader.indexOf("negative_reviews_points")
  for (let i = 1; i < ratingLines.length; i += 1) {
    const cols = parseCsvLine(ratingLines[i])
    const id = cols[ratingIdIdx] ?? ""
    const raw = cols[ratingIdx] ?? ""
    const num = raw ? Number(raw) : NaN
    ratings.set(id, Number.isFinite(num) ? num : null)
    reviewPoints.set(id, {
      positive: cols[positivePointsIdx] ?? "",
      negative: cols[negativePointsIdx] ?? "",
    })
  }

  const capabilities = new Map<
    string,
    { hasOnlineOrdering: boolean | null; hasPickup: boolean | null; hasCatering: boolean | null }
  >()
  const capabilityLines = capabilityCsv.split(/\r?\n/).filter(Boolean)
  const capabilityHeader = parseCsvLine(capabilityLines[0] ?? "")
  const capabilityIdIdx = capabilityHeader.indexOf("restaurant_id")
  const onlineIdx = capabilityHeader.indexOf("has_online_ordering")
  const pickupIdx = capabilityHeader.indexOf("has_pickup")
  const cateringIdx = capabilityHeader.indexOf("has_catering")

  const parseYesNo = (v: string): boolean | null => {
    const normalized = (v ?? "").trim().toLowerCase()
    if (normalized === "yes") return true
    if (normalized === "no") return false
    return null
  }

  for (let i = 1; i < capabilityLines.length; i += 1) {
    const cols = parseCsvLine(capabilityLines[i])
    const id = cols[capabilityIdIdx] ?? ""
    capabilities.set(id, {
      hasOnlineOrdering: parseYesNo(cols[onlineIdx] ?? ""),
      hasPickup: parseYesNo(cols[pickupIdx] ?? ""),
      hasCatering: parseYesNo(cols[cateringIdx] ?? ""),
    })
  }

  const descriptions = new Map<string, string>()
  const descriptionLines = descriptionCsv.split(/\r?\n/).filter(Boolean)
  const descriptionHeader = parseCsvLine(descriptionLines[0] ?? "")
  const descriptionIdIdx = descriptionHeader.indexOf("restaurant_id")
  const descriptionTextIdx = descriptionHeader.indexOf("description_50_words")
  for (let i = 1; i < descriptionLines.length; i += 1) {
    const cols = parseCsvLine(descriptionLines[i])
    const id = cols[descriptionIdIdx] ?? ""
    const text = cols[descriptionTextIdx] ?? ""
    descriptions.set(id, text)
  }

  const orderSetupMap = new Map<string, { first: string; all: string }>()
  const orderLines = orderSetupCsv.split(/\r?\n/).filter(Boolean)
  const orderHeader = parseCsvLine(orderLines[0] ?? "")
  const orderIdIdx = orderHeader.indexOf("restaurant_id")
  const firstPlatformIdx = orderHeader.indexOf("first_order_platform")
  const allPlatformsIdx = orderHeader.indexOf("all_order_platforms")
  for (let i = 1; i < orderLines.length; i += 1) {
    const cols = parseCsvLine(orderLines[i])
    const id = cols[orderIdIdx] ?? ""
    orderSetupMap.set(id, {
      first: cols[firstPlatformIdx] ?? "",
      all: cols[allPlatformsIdx] ?? "",
    })
  }

  const fitScoreMap = new Map<string, number | null>()
  const fitLines = fitRankingCsv.split(/\r?\n/).filter(Boolean)
  const fitHeader = parseCsvLine(fitLines[0] ?? "")
  const fitIdIdx = fitHeader.indexOf("restaurant_id")
  const fitScoreIdx = fitHeader.indexOf("total_score")
  for (let i = 1; i < fitLines.length; i += 1) {
    const cols = parseCsvLine(fitLines[i])
    const id = cols[fitIdIdx] ?? ""
    const raw = cols[fitScoreIdx] ?? ""
    const num = raw ? Number(raw) : NaN
    fitScoreMap.set(id, Number.isFinite(num) ? num : null)
  }

  const lines = restaurantCsv.split(/\r?\n/).filter(Boolean)
  const header = parseCsvLine(lines[0] ?? "")
  const idIdx = header.indexOf("restaurant_id")
  const nameIdx = header.indexOf("name")
  const cuisineIdx = header.indexOf("cuisine_type")
  const cityIdx = header.indexOf("city")
  const stateIdx = header.indexOf("state")

  const rows: RestaurantRow[] = []
  for (let i = 1; i < lines.length; i += 1) {
    const cols = parseCsvLine(lines[i])
    const id = cols[idIdx] ?? ""
    const name = cols[nameIdx] ?? ""
    const cuisine = cols[cuisineIdx] ?? ""
    const city = cols[cityIdx] ?? ""
    const state = cols[stateIdx] ?? ""
    rows.push({
      id,
      name,
      cuisine,
      location: [city, state].filter(Boolean).join(", "),
      rating: ratings.get(id) ?? null,
      hasOnlineOrdering: capabilities.get(id)?.hasOnlineOrdering ?? null,
      hasPickup: capabilities.get(id)?.hasPickup ?? null,
      hasCatering: capabilities.get(id)?.hasCatering ?? null,
      description: descriptions.get(id) ?? "",
      positiveReviewPoints: reviewPoints.get(id)?.positive ?? "",
      negativeReviewPoints: reviewPoints.get(id)?.negative ?? "",
      firstOrderPlatform: orderSetupMap.get(id)?.first ?? "",
      allOrderPlatforms: orderSetupMap.get(id)?.all ?? "",
      fitScore: fitScoreMap.get(id) ?? null,
    })
  }
  return rows
}

export default async function Page() {
  const restaurants = await loadRestaurants()
  return <DashboardView data={dashboardData} restaurants={restaurants} />
}
